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
package kafka.server.share.dlq.integration;

import kafka.server.share.dlq.DLQProducerFactory;
import kafka.server.share.dlq.DLQTopicManager;
import kafka.server.share.dlq.DLQWriter;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for DLQ functionality.
 *
 * Tests use embedded Kafka cluster to validate:
 * - Complete DLQ flow from write to read
 * - Topic auto-creation
 * - Metadata header preservation
 * - Multiple concurrent writes
 * - Broker failures and recovery
 *
 * Test Pattern: Given-When-Then with real Kafka
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DLQEndToEndIntegrationTest {

    private KafkaClusterTestKit cluster;
    private String bootstrapServers;
    private ShareGroupConfig config;
    private DLQWriter dlqWriter;
    private DLQTopicManager topicManager;
    private Admin adminClient;

    private static final int BROKER_ID = 1;
    private static final String SOURCE_TOPIC = "test-orders";
    private static final String DLQ_TOPIC_PREFIX = "__test_dlq_";

    @BeforeAll
    void setupCluster() throws Exception {
        // Create embedded Kafka cluster with 3 brokers
        TestKitNodes nodes = new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(1)
                .build();

        cluster = new KafkaClusterTestKit.Builder(nodes).build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();

        bootstrapServers = cluster.bootstrapServers();

        // Setup DLQ infrastructure
        setupDLQInfrastructure();

        // Create source topic
        createSourceTopic(SOURCE_TOPIC, 3);
    }

    @AfterAll
    void teardownCluster() throws Exception {
        if (dlqWriter != null) {
            dlqWriter.close();
        }
        if (topicManager != null) {
            topicManager.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    @BeforeEach
    void cleanupTopics() throws Exception {
        // Delete DLQ topics between tests
        Set<String> topics = adminClient.listTopics().names().get();
        Set<String> dlqTopics = topics.stream()
                .filter(t -> t.startsWith(DLQ_TOPIC_PREFIX))
                .collect(Collectors.toSet());

        if (!dlqTopics.isEmpty()) {
            adminClient.deleteTopics(dlqTopics).all().get();
        }

        // Clear topic manager cache
        topicManager.clearCache();
    }

    // ========== Basic DLQ Flow Tests ==========

    @Test
    @DisplayName("End-to-end: Reject record -> appears in DLQ with headers")
    void testRejectRecordWritesToDLQWithHeaders() throws Exception {
        // Given: A rejected record
        String key = "order-123";
        String value = "{\"orderId\":\"123\",\"amount\":99.99}";
        String groupId = "test-group";
        String memberId = "consumer-1";
        int deliveryCount = 3;
        String failureReason = "ValidationError";

        // When: Write to DLQ
        CompletableFuture<RecordMetadata> writeFuture = dlqWriter.writeToDLQ(
                SOURCE_TOPIC,
                0, // partition
                12345L, // offset
                key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8),
                new Header[0],
                groupId,
                memberId,
                deliveryCount,
                failureReason
        );

        RecordMetadata metadata = writeFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(metadata);

        // Then: Record appears in DLQ topic
        String dlqTopic = DLQ_TOPIC_PREFIX + SOURCE_TOPIC;

        List<ConsumerRecord<byte[], byte[]>> dlqRecords = consumeDLQTopic(dlqTopic, 1);
        assertEquals(1, dlqRecords.size());

        ConsumerRecord<byte[], byte[]> dlqRecord = dlqRecords.get(0);

        // Verify record content
        assertEquals(key, new String(dlqRecord.key(), StandardCharsets.UTF_8));
        assertEquals(value, new String(dlqRecord.value(), StandardCharsets.UTF_8));

        // Verify all metadata headers
        assertHeaderEquals(dlqRecord, "__ share.dlq.original.topic", SOURCE_TOPIC);
        assertHeaderEquals(dlqRecord, "__share.dlq.original.partition", "0");
        assertHeaderEquals(dlqRecord, "__share.dlq.original.offset", "12345");
        assertHeaderEquals(dlqRecord, "__share.dlq.group.id", groupId);
        assertHeaderEquals(dlqRecord, "__share.dlq.member.id", memberId);
        assertHeaderEquals(dlqRecord, "__share.dlq.delivery.count", String.valueOf(deliveryCount));
        assertHeaderEquals(dlqRecord, "__share.dlq.broker.id", String.valueOf(BROKER_ID));
        assertHeaderEquals(dlqRecord, "__share.dlq.failure.reason", failureReason);

        // Verify timestamp header exists and is recent
        Header timestampHeader = getHeader(dlqRecord, "__share.dlq.reject.timestamp");
        assertNotNull(timestampHeader);
    }

    @Test
    @DisplayName("DLQ topic auto-created on first rejection")
    void testDLQTopicAutoCreated() throws Exception {
        // Given: DLQ topic doesn't exist
        String dlqTopic = DLQ_TOPIC_PREFIX + SOURCE_TOPIC;
        Set<String> existingTopics = adminClient.listTopics().names().get();
        assertFalse(existingTopics.contains(dlqTopic),
                "DLQ topic should not exist before first rejection");

        // When: Write first rejection
        dlqWriter.writeToDLQ(
                SOURCE_TOPIC, 0, 100L,
                "key".getBytes(), "value".getBytes(),
                new Header[0],
                "group", "member", 1, null
        ).get(10, TimeUnit.SECONDS);

        // Then: DLQ topic created
        Set<String> topicsAfter = adminClient.listTopics().names().get();
        assertTrue(topicsAfter.contains(dlqTopic),
                "DLQ topic should be auto-created");

        // Verify topic configuration
        var topicDescription = adminClient.describeTopics(Collections.singleton(dlqTopic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS);

        assertEquals(1, topicDescription.get(dlqTopic).partitions().size(),
                "DLQ topic should have 1 partition by default");
    }

    @Test
    @DisplayName("Multiple rejections preserve order in DLQ")
    void testMultipleRejectionsPreserveOrder() throws Exception {
        // Given: Multiple records to reject
        int recordCount = 10;
        List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();

        // When: Reject multiple records in sequence
        for (int i = 0; i < recordCount; i++) {
            CompletableFuture<RecordMetadata> future = dlqWriter.writeToDLQ(
                    SOURCE_TOPIC,
                    0, // same partition
                    100L + i, // sequential offsets
                    ("key-" + i).getBytes(),
                    ("value-" + i).getBytes(),
                    new Header[0],
                    "group", "member", 1, null
            );
            futures.add(future);
        }

        // Wait for all writes
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);

        // Then: Records in DLQ maintain order
        String dlqTopic = DLQ_TOPIC_PREFIX + SOURCE_TOPIC;
        List<ConsumerRecord<byte[], byte[]>> dlqRecords = consumeDLQTopic(dlqTopic, recordCount);

        assertEquals(recordCount, dlqRecords.size());

        // Verify sequential offsets
        for (int i = 0; i < recordCount; i++) {
            ConsumerRecord<byte[], byte[]> record = dlqRecords.get(i);
            String expectedKey = "key-" + i;
            String actualKey = new String(record.key(), StandardCharsets.UTF_8);
            assertEquals(expectedKey, actualKey,
                    "Records should be in order");
        }
    }

    // ========== Concurrent Operations Tests ==========

    @Test
    @DisplayName("Concurrent rejections from multiple threads")
    void testConcurrentRejections() throws Exception {
        // Given: Multiple threads rejecting concurrently
        int threadCount = 10;
        int recordsPerThread = 5;
        List<Thread> threads = new ArrayList<>();
        List<CompletableFuture<RecordMetadata>> allFutures = Collections.synchronizedList(new ArrayList<>());

        // When: Concurrent writes
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Thread thread = new Thread(() -> {
                for (int r = 0; r < recordsPerThread; r++) {
                    try {
                        CompletableFuture<RecordMetadata> future = dlqWriter.writeToDLQ(
                                SOURCE_TOPIC,
                                threadId % 3, // spread across partitions
                                (threadId * recordsPerThread + r),
                                ("key-" + threadId + "-" + r).getBytes(),
                                ("value-" + threadId + "-" + r).getBytes(),
                                new Header[0],
                                "group", "member-" + threadId, 1, null
                        );
                        allFutures.add(future);
                    } catch (Exception e) {
                        fail("Concurrent write failed: " + e.getMessage());
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join(30000);
        }

        // Then: All writes succeed
        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);

        assertEquals(threadCount * recordsPerThread, allFutures.size());

        // Verify all records in DLQ
        String dlqTopic = DLQ_TOPIC_PREFIX + SOURCE_TOPIC;
        List<ConsumerRecord<byte[], byte[]>> dlqRecords =
                consumeDLQTopic(dlqTopic, threadCount * recordsPerThread);

        assertEquals(threadCount * recordsPerThread, dlqRecords.size(),
                "All rejected records should be in DLQ");
    }

    // ========== Multiple Source Topics Tests ==========

    @Test
    @DisplayName("Multiple source topics create separate DLQ topics")
    void testMultipleSourceTopicsCreateSeparateDLQs() throws Exception {
        // Given: Multiple source topics
        String[] sourceTopics = {"orders", "payments", "shipments"};
        for (String topic : sourceTopics) {
            createSourceTopic(topic, 1);
        }

        // When: Reject records from each topic
        for (String sourceTopic : sourceTopics) {
            dlqWriter.writeToDLQ(
                    sourceTopic, 0, 100L,
                    "key".getBytes(), "value".getBytes(),
                    new Header[0],
                    "group", "member", 1, null
            ).get(10, TimeUnit.SECONDS);
        }

        // Then: Separate DLQ topic for each source
        Set<String> topics = adminClient.listTopics().names().get();

        for (String sourceTopic : sourceTopics) {
            String dlqTopic = DLQ_TOPIC_PREFIX + sourceTopic;
            assertTrue(topics.contains(dlqTopic),
                    "DLQ topic should exist for " + sourceTopic);

            // Verify record in correct DLQ
            List<ConsumerRecord<byte[], byte[]>> records = consumeDLQTopic(dlqTopic, 1);
            assertEquals(1, records.size());
            assertHeaderEquals(records.get(0), "__share.dlq.original.topic", sourceTopic);
        }
    }

    // ========== Metrics and Observability Tests ==========

    @Test
    @DisplayName("Metrics track successful and failed writes")
    void testMetricsTracking() throws Exception {
        // Given: Initial metrics
        long initialSent = dlqWriter.getRecordsSent();
        long initialFailed = dlqWriter.getRecordsFailed();

        // When: Write successful records
        for (int i = 0; i < 5; i++) {
            dlqWriter.writeToDLQ(
                    SOURCE_TOPIC, 0, 100L + i,
                    "key".getBytes(), "value".getBytes(),
                    new Header[0],
                    "group", "member", 1, null
            ).get(10, TimeUnit.SECONDS);
        }

        // Then: Metrics updated
        assertEquals(initialSent + 5, dlqWriter.getRecordsSent());
        assertEquals(initialFailed, dlqWriter.getRecordsFailed());
        assertTrue(dlqWriter.getBytesSent() > 0);
    }

    // ========== Helper Methods ==========

    private void setupDLQInfrastructure() {
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, false);
        props.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 5);
        props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 30000);
        props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, true);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG, DLQ_TOPIC_PREFIX);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG, (short) 1); // Single replica for tests
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG, 1);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_INCLUDE_HEADERS_CONFIG, true);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_CONFIG, true);

        AbstractConfig abstractConfig = new AbstractConfig(ShareGroupConfig.CONFIG_DEF, props);
        config = new ShareGroupConfig(abstractConfig);

        adminClient = DLQProducerFactory.createDLQAdminClient(bootstrapServers);
        topicManager = new DLQTopicManager(adminClient, config);

        KafkaProducer<byte[], byte[]> producer = DLQProducerFactory.createDLQProducer(bootstrapServers, config);
        dlqWriter = new DLQWriter(producer, config, topicManager, BROKER_ID);
    }

    private void createSourceTopic(String topic, int partitions) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (Admin admin = Admin.create(props)) {
            admin.createTopics(Collections.singleton(
                    new org.apache.kafka.clients.admin.NewTopic(topic, partitions, (short) 1)
            )).all().get(10, TimeUnit.SECONDS);
        }
    }

    private List<ConsumerRecord<byte[], byte[]>> consumeDLQTopic(String topic, int expectedRecords) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(topic));

            long startTime = System.currentTimeMillis();
            long timeout = 30000; // 30 seconds

            while (records.size() < expectedRecords &&
                    System.currentTimeMillis() - startTime < timeout) {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(1000));
                polled.forEach(records::add);
            }
        }

        return records;
    }

    private void assertHeaderEquals(ConsumerRecord<byte[], byte[]> record,
                                     String headerKey, String expectedValue) {
        Header header = getHeader(record, headerKey);
        assertNotNull(header, "Header " + headerKey + " should be present");
        String actualValue = new String(header.value(), StandardCharsets.UTF_8);
        assertEquals(expectedValue, actualValue,
                "Header " + headerKey + " value mismatch");
    }

    private Header getHeader(ConsumerRecord<byte[], byte[]> record, String key) {
        for (Header header : record.headers()) {
            if (header.key().equals(key)) {
                return header;
            }
        }
        return null;
    }
}
