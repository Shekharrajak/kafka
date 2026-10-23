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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class ShareSinkConnectorsIntegrationTest {
    private static final String NAME = "native-share-test";
    private static final String TOPIC = "share-events";
    private static final Map<String, Integer> WRITES = new ConcurrentHashMap<>();
    private static final Set<String> FAILED_BATCH = ConcurrentHashMap.newKeySet();
    private static final AtomicBoolean FAIL_NEXT_WRITE = new AtomicBoolean();
    private EmbeddedConnectCluster connect;

    @BeforeEach
    void setup() throws Exception {
        WRITES.clear();
        FAILED_BATCH.clear();
        FAIL_NEXT_WRITE.set(false);
        Properties brokers = new Properties();
        brokers.put("share.coordinator.state.topic.replication.factor", "1");
        brokers.put("share.coordinator.state.topic.min.isr", "1");
        brokers.put("share.coordinator.state.topic.num.partitions", "1");
        brokers.put("group.share.min.record.lock.duration.ms", "1000");
        brokers.put("group.share.record.lock.duration.ms", "5000");
        connect = new EmbeddedConnectCluster.Builder().name(NAME).numWorkers(1)
                .workerProps(new HashMap<>(Map.of("connector.client.config.override.policy", "All")))
                .brokerProps(brokers).build();
        connect.start();
        connect.kafka().createTopic(TOPIC, 1);
        try (Admin admin = connect.kafka().createAdminClient()) {
            ConfigResource group = new ConfigResource(ConfigResource.Type.GROUP, "connect-share-" + NAME);
            admin.incrementalAlterConfigs(Map.of(group, List.of(new AlterConfigOp(
                    new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET))))
                    .all().get(30, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void cleanup() {
        if (connect != null) {
            connect.stop();
        }
        RuntimeHandles.get().deleteConnector(NAME);
    }

    private Map<String, String> config(int tasks) {
        return new HashMap<>(Map.of("connector.class", AuditingShareSinkConnector.class.getName(),
                "topics", TOPIC, "tasks.max", Integer.toString(tasks), "consumer.type", "share",
                "consumer.override.max.poll.records", "10"));
    }

    @Test
    void runsMoreTasksThanPartitionsAndPreservesProgressAcrossTaskReconfiguration() throws Exception {
        Map<String, String> config = config(3);
        connect.configureConnector(NAME, config);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(NAME, 3, "Share tasks did not start");
        for (int i = 0; i < 100; i++) {
            connect.kafka().produce(TOPIC, Integer.toString(i));
        }
        waitForCondition(() -> WRITES.size() == 100, 30000, "Share tasks did not deliver all records");
        config.put("tasks.max", "6");
        connect.configureConnector(NAME, config);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(NAME, 6, "Share tasks did not scale");
        for (int i = 100; i < 200; i++) {
            connect.kafka().produce(TOPIC, Integer.toString(i));
        }
        waitForCondition(() -> WRITES.size() == 200, 30000, "Records were lost after scaling");
        assertEquals(IntStream.range(0, 200).mapToObj(Integer::toString).collect(Collectors.toSet()), WRITES.keySet());
    }

    @Test
    void replaysAfterSinkSideEffectWithoutAcknowledgement() throws Exception {
        FAIL_NEXT_WRITE.set(true);
        connect.configureConnector(NAME, config(1));
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(NAME, 1, "Share task did not start");
        for (int i = 0; i < 20; i++) {
            connect.kafka().produce(TOPIC, Integer.toString(i));
        }
        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(NAME, 1, "Ambiguous write did not fail the task");
        assertFalse(FAILED_BATCH.isEmpty());
        connect.restartTask(NAME, 0);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(NAME, 1, "Share task did not restart");
        waitForCondition(() -> WRITES.size() == 20 && FAILED_BATCH.stream().allMatch(value -> WRITES.get(value) >= 2),
                30000, "Unacknowledged writes were not redelivered");
        assertEquals(IntStream.range(0, 20).mapToObj(Integer::toString).collect(Collectors.toSet()), WRITES.keySet());
        assertTrue(WRITES.values().stream().anyMatch(count -> count > 1));
    }

    @Test
    void rejectsSinksWithoutTheDurableWriteContract() throws Exception {
        Map<String, String> config = config(1);
        config.put("connector.class", TestableSinkConnector.class.getName());
        connect.configureConnector(NAME, config);
        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(NAME, 1, "Ordinary SinkTask was accepted in share mode");
    }

    public static class AuditingShareSinkConnector extends SinkConnector {
        private Map<String, String> config;

        @Override
        public void start(Map<String, String> props) {
            config = props;
        }

        @Override
        public Class<? extends Task> taskClass() {
            return AuditingShareSinkTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            List<Map<String, String>> configs = new ArrayList<>();
            for (int i = 0; i < maxTasks; i++) {
                configs.add(new HashMap<>(config));
            }
            return configs;
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
            return "test";
        }
    }

    public static class AuditingShareSinkTask extends ShareSinkTask {
        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            for (SinkRecord record : records) {
                WRITES.merge(record.value().toString(), 1, Integer::sum);
            }
            if (FAIL_NEXT_WRITE.compareAndSet(true, false)) {
                records.forEach(record -> FAILED_BATCH.add(record.value().toString()));
                throw new RetriableException("Simulated sink side effect followed by a failed response");
            }
        }

        @Override
        public void stop() {
        }

        @Override
        public String version() {
            return "test";
        }
    }
}
