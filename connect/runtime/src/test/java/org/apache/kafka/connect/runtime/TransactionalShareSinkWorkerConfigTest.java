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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfigTest.MOCK_PLUGINS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransactionalShareSinkWorkerConfigTest {
    @Test
    public void testPrivateClientsUseSinkOverridesAndEnforceTransactionProperties() {
        WorkerConfig worker = mock(WorkerConfig.class);
        when(worker.bootstrapServers()).thenReturn("worker:9092");
        when(worker.groupId()).thenReturn("connect-workers");
        when(worker.originalsWithPrefix("producer.")).thenReturn(Map.of("bootstrap.servers", "output:9092", "enable.idempotence", "false"));
        when(worker.originalsWithPrefix("consumer.")).thenReturn(Map.of("bootstrap.servers", "input:9092", "group.id", "unsafe-group"));
        ConnectorClientConfigOverridePolicy policy = mock(ConnectorClientConfigOverridePolicy.class);
        when(policy.validate(any())).thenReturn(List.of());
        SinkConnectorConfig sink = new SinkConnectorConfig(MOCK_PLUGINS, Map.of(
            "name", "sink", "connector.class", ConnectorConfigTest.TestConnector.class.getName(), "topics", "input",
            "consumer.type", "share", "share.delivery.guarantee", "exactly_once", "share.group.id", "dedicated-input"));
        ConnectorTaskId id = new ConnectorTaskId("sink", 2);
        Map<String, Object> consumer = Worker.transactionalShareSinkConsumerConfigs(id, worker, sink, SinkConnector.class, policy, "cluster");
        Map<String, Object> producer = Worker.transactionalShareSinkProducerConfigs(id, worker, sink, SinkConnector.class, policy, "cluster");
        assertEquals("input:9092", consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("dedicated-input", consumer.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("transactional", consumer.get(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG));
        assertEquals("record_limit", consumer.get(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG));
        assertFalse(consumer.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertFalse(consumer.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("output:9092", producer.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("true", producer.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", producer.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(Worker.transactionalShareSinkTaskTransactionalId("connect-workers", "sink", 2), producer.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        Map<String, Object> parsedConsumer = assertDoesNotThrow(() -> ConsumerConfig.configDef().parse(consumer));
        assertEquals(10000, parsedConsumer.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
        assertEquals(5000, parsedConsumer.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        ProducerConfig parsed = assertDoesNotThrow(() -> new ProducerConfig(producer));
        assertTrue(parsed.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG) <= 10000);
        assertTrue(parsed.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG) <= 10000);
        ArgumentCaptor<ConnectorClientConfigRequest> requests = ArgumentCaptor.forClass(ConnectorClientConfigRequest.class);
        verify(policy, times(2)).validate(requests.capture());
        assertTrue(requests.getAllValues().stream().allMatch(request -> request.connectorType() == ConnectorType.SINK));
    }

    @Test
    public void testSinkProducerIdsAreStableDedicatedAndUnambiguous() {
        String sink = Worker.transactionalShareSinkTaskTransactionalId("workers", "sink", 0);
        assertEquals(sink, Worker.transactionalShareSinkTaskTransactionalId("workers", "sink", 0));
        assertNotEquals(sink, Worker.transactionalShareSinkTaskTransactionalId("workers", "sink", 1));
        assertNotEquals(sink, Worker.taskTransactionalId("workers", "sink", 0));
        assertNotEquals(Worker.transactionalShareSinkTaskTransactionalId("a-b", "c", 0),
            Worker.transactionalShareSinkTaskTransactionalId("a", "b-c", 0));
    }
}
