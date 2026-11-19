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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableWorkTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void putWaitsForEveryDestinationWriteBeforeReportingCompletion() throws Exception {
        WaitingProducer producer = new WaitingProducer();
        TaskMetrics metrics = new TaskMetrics("share", "0", "instance");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (DurableWork work = work(producer, metrics)) {
            Future<?> call = executor.submit(() -> work.put(List.of(record(1), record(2))));
            assertNotNull(producer.sent.poll(5, TimeUnit.SECONDS));
            assertFalse(call.isDone());
            assertTrue(metrics.render().contains("demo_task_processing{mode=\"share\",task=\"0\",instance=\"instance\",run_id=\"run\",partition=\"1\"} 1"));
            assertTrue(producer.completeNext());
            assertNotNull(producer.sent.poll(5, TimeUnit.SECONDS));
            assertFalse(call.isDone());
            assertTrue(producer.completeNext());
            call.get(5, TimeUnit.SECONDS);
            assertTrue(metrics.render().contains("demo_task_processed_total{mode=\"share\",task=\"0\",instance=\"instance\",run_id=\"run\",partition=\"1\"} 2"));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        assertTrue(producer.closed());
    }

    @Test
    void failedWriteCannotBecomeSuccessfulProcessing() throws Exception {
        WaitingProducer producer = new WaitingProducer();
        TaskMetrics metrics = new TaskMetrics("share", "0", "instance");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (DurableWork work = work(producer, metrics)) {
            Future<?> call = executor.submit(() -> work.put(List.of(record(1))));
            assertNotNull(producer.sent.poll(5, TimeUnit.SECONDS));
            assertTrue(producer.errorNext(new IllegalStateException("destination unavailable")));
            ExecutionException failure = assertThrows(ExecutionException.class, () -> call.get(5, TimeUnit.SECONDS));
            assertInstanceOf(ConnectException.class, failure.getCause());
            assertTrue(metrics.render().contains("demo_task_processed_total{mode=\"share\",task=\"0\",instance=\"instance\",run_id=\"run\",partition=\"1\"} 0"));
            assertTrue(metrics.render().contains("demo_task_failures_total{mode=\"share\",task=\"0\",instance=\"instance\",run_id=\"run\",partition=\"1\"} 1"));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptedDestinationWaitPreservesInterruptAndFailsThePut() throws Exception {
        WaitingProducer producer = new WaitingProducer();
        AtomicReference<Thread> thread = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread worker = new Thread(runnable, "interrupted-demo-put");
            thread.set(worker);
            return worker;
        });
        try (DurableWork work = work(producer, new TaskMetrics("share", "0", "instance"))) {
            Future<Boolean> call = executor.submit(() -> {
                assertThrows(ConnectException.class, () -> work.put(List.of(record(1))));
                return Thread.currentThread().isInterrupted();
            });
            assertNotNull(producer.sent.poll(5, TimeUnit.SECONDS));
            thread.get().interrupt();
            assertTrue(call.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void retriesRetainInputIdentityAndPreserveEveryOutputOccurrence() throws Exception {
        MockProducer<String, String> producer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        SinkRecord input = record(7);
        SinkRecord routed = input.newRecord("routed", 2, input.keySchema(), input.key(),
                input.valueSchema(), input.value(), input.timestamp());
        try (DurableWork work = work(producer, new TaskMetrics("share", "0", "instance"))) {
            work.put(List.of(routed));
            work.put(List.of(routed));
            assertEquals(2, producer.history().size());
            JsonNode first = JSON.readTree(producer.history().get(0).value());
            JsonNode second = JSON.readTree(producer.history().get(1).value());
            assertEquals("input", first.path("inputTopic").asText());
            assertEquals(1, first.path("inputPartition").asInt());
            assertEquals(7, first.path("inputOffset").asLong());
            assertEquals("DOCUMENT-7", first.path("result").asText());
            assertEquals(0, first.path("attempt").asLong());
            assertEquals(1, second.path("attempt").asLong());
            assertEquals("run:7", producer.history().get(0).key());
            assertEquals("routed", routed.topic());
            assertEquals(input.value(), routed.value());
        }
    }

    @Test
    void invalidWorkCannotWriteOutput() {
        MockProducer<String, String> producer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        SinkRecord invalid = new SinkRecord("input", 1, null, null, Schema.STRING_SCHEMA,
                record(1).value().toString().replace("\"workMs\":0", "\"workMs\":30001"), 1);
        try (DurableWork work = work(producer, new TaskMetrics("share", "0", "instance"))) {
            assertThrows(ConnectException.class, () -> work.put(List.of(invalid)));
            assertTrue(producer.history().isEmpty());
        }
    }

    private static DurableWork work(MockProducer<String, String> producer, TaskMetrics metrics) {
        return new DurableWork("share", "0", "instance", "outputs", producer, metrics, () -> 1000);
    }

    private static SinkRecord record(int id) {
        String value = "{\"runId\":\"run\",\"jobId\":" + id + ",\"customerId\":\"customer\","
                + "\"createdAtMs\":100,\"workMs\":0,\"payload\":\"document-" + id + "\"}";
        return new SinkRecord("input", 1, Schema.STRING_SCHEMA, "run:" + id, Schema.STRING_SCHEMA, value, id);
    }

    private static final class WaitingProducer extends MockProducer<String, String> {
        private final BlockingQueue<ProducerRecord<String, String>> sent = new LinkedBlockingQueue<>();

        private WaitingProducer() {
            super(false, null, new StringSerializer(), new StringSerializer());
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record) {
            Future<RecordMetadata> result = super.send(record);
            sent.add(record);
            return result;
        }
    }
}
