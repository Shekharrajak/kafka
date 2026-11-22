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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    controllers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true"),
        @ClusterConfigProperty(key = "unstable.feature.versions.enable", value = "true"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "2"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3"),
        @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "2"),
        @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "3")
    }
)
public class ShareConsumerTransactionTest extends ShareConsumerTestBase {

    public ShareConsumerTransactionTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest(features = {
        @ClusterFeature(feature = Feature.SHARE_VERSION, version = 3),
        @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)
    })
    public void testCommitMappedOutputAndFilteredInput() throws Exception {
        verifyCommitMappedOutputAndFilteredInput(false);
    }

    @ClusterTest(features = {
        @ClusterFeature(feature = Feature.SHARE_VERSION, version = 3),
        @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)
    })
    public void testSourceLeaderLossAfterCommit() throws Exception {
        verifyCommitMappedOutputAndFilteredInput(true);
    }

    private void verifyCommitMappedOutputAndFilteredInput(boolean shutDownSourceLeader) throws Exception {
        TopicPartition input = new TopicPartition("transaction-input", 0);
        TopicPartition output = new TopicPartition("transaction-output", 0);
        for (TopicPartition partition : List.of(input, output)) {
            cluster.createTopic(partition.topic(), 1, (short) 3, Map.of("min.insync.replicas", "2"));
        }
        String groupId = "transaction-share-group";
        alterShareAutoOffsetReset(groupId, "earliest");
        alterShareIsolationLevel(groupId, "read_committed");
        Map<String, Object> consumerConfig = Map.of(
            ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "transactional",
            ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1
        );
        try (Producer<byte[], byte[]> inputProducer = createProducer();
             KafkaProducer<byte[], byte[]> producer = (KafkaProducer<byte[], byte[]>) cluster.<byte[], byte[]>producer(
                 Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-share-task"));
             ShareConsumer<byte[], byte[]> consumer = createShareConsumer(groupId, consumerConfig);
             var admin = createAdminClient();
             Consumer<byte[], byte[]> outputConsumer = cluster.consumer(Map.of(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))) {
            inputProducer.send(new ProducerRecord<>(input.topic(), null, new byte[] {1})).get();
            inputProducer.send(new ProducerRecord<>(input.topic(), null, new byte[] {2})).get();
            producer.initTransactions();
            consumer.setTransactionContextProvider(producer);
            consumer.subscribe(List.of(input.topic()));

            ConsumerRecord<byte[], byte[]> first = waitedPoll(consumer, 1000L, 1).iterator().next();
            assertEquals(0L, first.offset());
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(output.topic(), new byte[] {1}, new byte[] {11}));
            consumer.acknowledge(first, AcknowledgeType.ACCEPT);
            producer.commitTransaction();

            if (shutDownSourceLeader) {
                int originalLeader = topicPartitionLeader(admin, input.topic(), input.partition()).get(0);
                cluster.shutdownBroker(originalLeader);
                TestUtils.waitForCondition(() -> topicPartitionLeader(admin, input.topic(), input.partition())
                    .stream().anyMatch(leader -> leader != originalLeader), "The source partition did not elect a new leader");
            }
            ConsumerRecord<byte[], byte[]> filtered = waitedPoll(consumer, 1000L, 1).iterator().next();
            assertEquals(1L, filtered.offset());
            producer.beginTransaction();
            consumer.acknowledge(filtered, AcknowledgeType.ACCEPT);
            producer.commitTransaction();

            verifySharePartitionLag(admin, groupId, input, 0);
            assertTrue(consumer.poll(Duration.ofSeconds(2)).isEmpty());

            outputConsumer.assign(List.of(output));
            List<ConsumerRecord<byte[], byte[]>> outputRecords = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                outputConsumer.poll(Duration.ofMillis(250)).forEach(outputRecords::add);
                return !outputRecords.isEmpty();
            }, "The committed output was not visible");
            outputConsumer.poll(Duration.ofSeconds(1)).forEach(outputRecords::add);
            assertEquals(1, outputRecords.size());
            assertArrayEquals(new byte[] {1}, outputRecords.get(0).key());
            assertArrayEquals(new byte[] {11}, outputRecords.get(0).value());
        }
    }
}
