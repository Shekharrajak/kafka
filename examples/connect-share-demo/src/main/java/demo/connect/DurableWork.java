/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package demo.connect;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

final class DurableWork implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final String mode;
    private final String taskId;
    private final String instanceId;
    private final String outputTopic;
    private final Producer<String, String> producer;
    private final TaskMetrics metrics;
    private final LongSupplier clock;
    private long attempt;

    static DurableWork open(Map<String, String> props, String expectedMode) {
        if (!expectedMode.equals(props.get(DemoSinkConnector.MODE))) {
            throw new ConnectException("Demo task does not match its configured consumer mode");
        }
        String taskId = required(props, DemoSinkConnector.TASK_ID);
        String outputTopic = required(props, DemoSinkConnector.OUTPUT_TOPIC);
        String bootstrapServers = required(props, DemoSinkConnector.BOOTSTRAP_SERVERS);
        String runId = required(props, "demo.run.id");
        String instanceId = UUID.randomUUID().toString();
        TaskMetrics metrics = TaskMetrics.open(expectedMode, taskId, instanceId);
        try {
            metrics.registerRun(runId);
            Map<String, Object> producerProps = Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.ACKS_CONFIG, "all",
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                    ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000,
                    ProducerConfig.CLIENT_ID_CONFIG, "demo-output-" + expectedMode + "-" + taskId);
            return new DurableWork(expectedMode, taskId, instanceId, outputTopic,
                    new KafkaProducer<>(producerProps), metrics, System::currentTimeMillis);
        } catch (RuntimeException | Error e) {
            metrics.close();
            throw e;
        }
    }

    DurableWork(String mode, String taskId, String instanceId, String outputTopic,
                Producer<String, String> producer, TaskMetrics metrics, LongSupplier clock) {
        this.mode = mode;
        this.taskId = taskId;
        this.instanceId = instanceId;
        this.outputTopic = outputTopic;
        this.producer = Objects.requireNonNull(producer);
        this.metrics = Objects.requireNonNull(metrics);
        this.clock = Objects.requireNonNull(clock);
    }

    void put(Collection<SinkRecord> records) {
        long currentAttempt = attempt++;
        for (SinkRecord record : records) {
            process(record, currentAttempt);
        }
    }

    private void process(SinkRecord record, long currentAttempt) {
        JsonNode job = parse(record.value());
        String runId = text(job, "runId");
        int partition = record.originalKafkaPartition();
        String inputTopic = record.originalTopic();
        if (partition < 0 || partition > 2 || inputTopic == null) {
            throw new ConnectException("Demo inputs require an original topic and partition 0, 1 or 2");
        }
        int jobId = integer(job, "jobId", 0, Integer.MAX_VALUE);
        int workMs = integer(job, "workMs", 0, 30_000);
        long createdAtMs = job.path("createdAtMs").asLong(-1);
        if (createdAtMs < 0) {
            throw new ConnectException("Missing or invalid createdAtMs");
        }
        String payload = text(job, "payload");
        text(job, "customerId");
        metrics.begin(runId, partition);
        boolean succeeded = false;
        try {
            long started = clock.getAsLong();
            TimeUnit.MILLISECONDS.sleep(workMs);
            ObjectNode output = JSON.createObjectNode()
                    .put("runId", runId)
                    .put("jobId", jobId)
                    .put("createdAtMs", createdAtMs)
                    .put("payload", payload)
                    .put("result", payload.toUpperCase(Locale.ROOT))
                    .put("mode", mode)
                    .put("taskId", taskId)
                    .put("instanceId", instanceId)
                    .put("attempt", currentAttempt)
                    .put("inputTopic", inputTopic)
                    .put("inputPartition", partition)
                    .put("inputOffset", record.originalKafkaOffset())
                    .put("processingStartedMs", started)
                    .put("processingFinishedMs", clock.getAsLong());
            ProducerRecord<String, String> audit = new ProducerRecord<>(outputTopic, partition,
                    runId + ":" + jobId, output.toString());
            producer.send(audit).get(35, TimeUnit.SECONDS);
            succeeded = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Demo destination write interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new ConnectException("Demo destination write was not confirmed", e);
        } finally {
            metrics.end(runId, partition, succeeded);
        }
    }

    private static JsonNode parse(Object value) {
        if (!(value instanceof String input)) {
            throw new ConnectException("Demo records require StringConverter JSON values");
        }
        try {
            JsonNode job = JSON.readTree(input);
            if (job == null || !job.isObject()) {
                throw new ConnectException("Demo input must be a JSON object");
            }
            return job;
        } catch (IOException e) {
            throw new ConnectException("Invalid demo input JSON", e);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual()) {
            throw new ConnectException("Missing or invalid " + field);
        }
        return value.textValue();
    }

    private static int integer(JsonNode node, String field, int min, int max) {
        JsonNode value = node.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < min || value.intValue() > max) {
            throw new ConnectException("Missing or invalid " + field);
        }
        return value.intValue();
    }

    private static String required(Map<String, String> props, String field) {
        String value = props.get(field);
        if (value == null || value.isBlank()) {
            throw new ConnectException("Missing " + field);
        }
        return value;
    }

    @Override
    public void close() {
        try {
            producer.close(Duration.ofSeconds(5));
        } finally {
            metrics.close();
        }
    }
}
