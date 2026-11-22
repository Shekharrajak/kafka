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
package org.apache.kafka.connect.integration;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.TransactionalShareSinkTask;
import org.apache.kafka.connect.transforms.Filter;
import org.apache.kafka.connect.transforms.predicates.HasHeaderKey;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("integration")
public class TransactionalShareSinkIntegrationTest {
    private static final String CONNECTOR = "share-to-kafka";
    private static final String GROUP = "transactional-sink-group";
    private static final String INPUT = "sink-input";
    private static final String OUTPUT = "sink-output";
    private static final long TEST_WAIT_MS = 30_000;

    private EmbeddedConnectCluster connect;

    @BeforeEach
    void start() throws Exception {
        Properties brokerProps = new Properties();
        brokerProps.put("unstable.api.versions.enable", "true");
        brokerProps.put("unstable.feature.versions.enable", "true");
        brokerProps.put("transaction.state.log.replication.factor", "3");
        brokerProps.put("transaction.state.log.min.isr", "2");
        brokerProps.put("share.coordinator.state.topic.replication.factor", "3");
        brokerProps.put("share.coordinator.state.topic.min.isr", "2");
        brokerProps.put("share.coordinator.state.topic.num.partitions", "3");
        Map<String, String> workerProps = new HashMap<>(Map.of(
            DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled",
            DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG, "1000",
            WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All"
        ));
        connect = new EmbeddedConnectCluster.Builder().name("transactional-share")
            .numBrokers(3).numWorkers(2).brokerProps(brokerProps).workerProps(workerProps).build();
        connect.start();
        try (var admin = connect.kafka().createAdminClient()) {
            admin.updateFeatures(Map.of(
                "share.version", new FeatureUpdate((short) 3, FeatureUpdate.UpgradeType.UPGRADE),
                "transaction.version", new FeatureUpdate((short) 2, FeatureUpdate.UpgradeType.UPGRADE)
            ), new UpdateFeaturesOptions()).all().get(30, TimeUnit.SECONDS);
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP, GROUP), List.of(
                new AlterConfigOp(new ConfigEntry("share.isolation.level", "read_committed"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("share.record.lock.duration.ms", "30000"), AlterConfigOp.OpType.SET)
            ))).all().get(30, TimeUnit.SECONDS);
        }
        for (String topic : List.of(INPUT, OUTPUT)) {
            connect.kafka().createTopic(topic, 1, 3, Map.of("min.insync.replicas", "2"));
        }
    }

    @AfterEach
    void stop() {
        MappingTask.failAfterSend.countDown();
        if (connect != null) {
            connect.stop();
        }
    }

    @Test
    void testAbortedAttemptThenReplacementAndFilteredInput() throws Exception {
        MappingTask.FAIL_FIRST.set(true);
        MappingTask.failAfterSend = new CountDownLatch(1);
        Map<String, String> connectorProps = Map.ofEntries(
            Map.entry("connector.class", MappingConnector.class.getName()),
            Map.entry("tasks.max", "1"),
            Map.entry("topics", INPUT),
            Map.entry("consumer.type", "share"),
            Map.entry("share.group.id", GROUP),
            Map.entry("share.delivery.guarantee", "exactly_once"),
            Map.entry("consumer.override.max.poll.records", "1"),
            Map.entry("transforms", "drop"),
            Map.entry("transforms.drop.type", Filter.class.getName()),
            Map.entry("transforms.drop.predicate", "dropHeader"),
            Map.entry("predicates", "dropHeader"),
            Map.entry("predicates.dropHeader.type", HasHeaderKey.class.getName()),
            Map.entry("predicates.dropHeader.name", "drop")
        );
        connect.configureConnector(CONNECTOR, connectorProps);
        String firstWorkerId = awaitRunningTask(null);
        WorkerHandle firstWorker = connect.workers().stream()
            .filter(worker -> firstWorkerId.endsWith(":" + worker.url().getPort())).findFirst().orElseThrow();

        TopicPartition resultPartition = new TopicPartition(OUTPUT, 0);
        try (var inputProducer = connect.kafka().createProducer(Map.of());
             var uncommitted = connect.kafka().createConsumer(Map.of());
             var committed = connect.kafka().createConsumer(Map.of(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"));
             var admin = connect.kafka().createAdminClient()) {
            uncommitted.assign(List.of(resultPartition));
            committed.assign(List.of(resultPartition));
            inputProducer.send(new ProducerRecord<>(INPUT, 0, bytes("key"), bytes("mapped"))).get(30, TimeUnit.SECONDS);

            waitForCondition(() -> !uncommitted.poll(Duration.ofMillis(250)).isEmpty(), TEST_WAIT_MS,
                "The initial task did not append its output before the injected failure");
            MappingTask.failAfterSend.countDown();
            waitForCondition(() -> connect.connectorStatus(CONNECTOR).tasks().stream().anyMatch(task -> "FAILED".equals(task.state())),
                TEST_WAIT_MS, "The task did not fail after the injected error");
            assertTrue(connect.connectorStatus(CONNECTOR).tasks().get(0).trace().contains("Injected failure after the output send"));
            assertTrue(committed.poll(Duration.ofSeconds(1)).isEmpty());

            connect.removeWorker(firstWorker);
            awaitRunningTask(firstWorkerId);

            var filtered = new ProducerRecord<>(INPUT, 0, bytes("filtered"), bytes("discard"));
            filtered.headers().add("drop", bytes("true"));
            inputProducer.send(filtered).get(30, TimeUnit.SECONDS);

            List<String> outputs = new ArrayList<>();
            waitForCondition(() -> {
                committed.poll(Duration.ofMillis(250)).forEach(record -> outputs.add(new String(record.value(), StandardCharsets.UTF_8)));
                return !outputs.isEmpty();
            }, TEST_WAIT_MS, "The replacement task did not commit its output");
            TopicPartition sourcePartition = new TopicPartition(INPUT, 0);
            waitForCondition(() -> {
                var offsets = admin.listShareGroupOffsets(Map.of(GROUP, new ListShareGroupOffsetsSpec().topicPartitions(List.of(sourcePartition))))
                    .partitionsToOffsetInfo(GROUP).get(10, TimeUnit.SECONDS);
                return offsets.containsKey(sourcePartition) && offsets.get(sourcePartition).lag().orElse(-1L) == 0;
            }, TEST_WAIT_MS, "The mapped and filtered input records were not both acknowledged");
            committed.poll(Duration.ofSeconds(1)).forEach(record -> outputs.add(new String(record.value(), StandardCharsets.UTF_8)));
            assertEquals(List.of("mapped"), outputs);
        }
    }

    private String awaitRunningTask(String previousWorkerId) throws InterruptedException {
        AtomicReference<String> workerId = new AtomicReference<>();
        waitForCondition(() -> {
            var status = connect.connectorStatus(CONNECTOR);
            for (var task : status.tasks()) {
                if (task.workerId().equals(previousWorkerId)) continue;
                if ("FAILED".equals(task.state())) fail(task.trace());
                if ("RUNNING".equals(status.connector().state()) && "RUNNING".equals(task.state())) {
                    workerId.set(task.workerId());
                    return true;
                }
            }
            return false;
        }, TEST_WAIT_MS, "The transactional share task did not start");
        return workerId.get();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static class MappingConnector extends SinkConnector {
        private Map<String, String> props;

        @Override
        public void start(Map<String, String> props) {
            this.props = Map.copyOf(props);
        }

        @Override
        public Class<? extends Task> taskClass() {
            return MappingTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return List.of(props);
        }

        @Override
        public void stop() {
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public String version() {
            return "1";
        }
    }

    public static class MappingTask extends TransactionalShareSinkTask {
        private static final AtomicBoolean FAIL_FIRST = new AtomicBoolean();
        private static volatile CountDownLatch failAfterSend = new CountDownLatch(0);

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public void put(Collection<SinkRecord> records, Output output) {
            for (SinkRecord record : records) {
                output.send(new ProducerRecord<>(OUTPUT, bytes((String) record.key()), bytes((String) record.value())));
                if (FAIL_FIRST.compareAndSet(true, false)) {
                    try {
                        assertTrue(failAfterSend.await(15, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new ConnectException(e);
                    }
                    throw new ConnectException("Injected failure after the output send");
                }
            }
        }

        @Override
        public void stop() {
        }

        @Override
        public String version() {
            return "1";
        }
    }
}
