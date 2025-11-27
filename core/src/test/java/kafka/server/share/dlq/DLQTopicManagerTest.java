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
package kafka.server.share.dlq;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DLQTopicManager.
 *
 * Tests cover:
 * - Topic creation with correct configuration
 * - Caching behavior
 * - Concurrent creation handling
 * - Error scenarios
 * - Auto-create disabled handling
 */
@ExtendWith(MockitoExtension.class)
public class DLQTopicManagerTest {

    @Mock
    private Admin adminClient;

    @Mock
    private CreateTopicsResult createTopicsResult;

    private ShareGroupConfig config;
    private DLQTopicManager topicManager;

    @BeforeEach
    void setUp() {
        // Create test config with DLQ enabled
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, false);
        props.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 5);
        props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 30000);
        props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, true);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG, "__test_dlq_");
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG, (short) 3);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG, 1);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_RETENTION_MS_CONFIG, 604800000L);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_CONFIG, true);

        AbstractConfig abstractConfig = new AbstractConfig(ShareGroupConfig.CONFIG_DEF, props);
        config = new ShareGroupConfig(abstractConfig);

        topicManager = new DLQTopicManager(adminClient, config);
    }

    // ========== Topic Name Construction Tests ==========

    @Test
    void testBuildDLQTopicName() {
        // Given: Source topic name
        String sourceTopic = "orders";

        // When: Build DLQ topic name
        String dlqTopic = topicManager.buildDLQTopicName(sourceTopic);

        // Then: Verify correct naming
        assertEquals("__test_dlq_orders", dlqTopic,
                "DLQ topic should be prefix + source topic");
    }

    @Test
    void testBuildDLQTopicNameWithSpecialCharacters() {
        // Given: Source topic with special characters
        String sourceTopic = "orders.v1-prod";

        // When: Build DLQ topic name
        String dlqTopic = topicManager.buildDLQTopicName(sourceTopic);

        // Then: Verify special characters preserved
        assertEquals("__test_dlq_orders.v1-prod", dlqTopic);
    }

    // ========== Topic Creation Tests ==========

    @Test
    void testEnsureDLQTopicExists_CreatesTopicSuccessfully() throws Exception {
        // Given: Topic doesn't exist, admin client succeeds
        KafkaFuture<Void> successFuture = KafkaFuture.completedFuture(null);
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(successFuture);

        // When: Ensure topic exists
        CompletableFuture<Void> result = topicManager.ensureDLQTopicExists("orders");

        // Then: Should complete successfully
        assertDoesNotThrow(() -> result.get());
        assertTrue(topicManager.isDLQTopicCreated("orders"),
                "Topic should be marked as created in cache");

        // Verify correct topic configuration
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(adminClient).createTopics(captor.capture());

        NewTopic createdTopic = captor.getValue().iterator().next();
        assertEquals("__test_dlq_orders", createdTopic.name());
        assertEquals(1, createdTopic.numPartitions());
        assertEquals((short) 3, createdTopic.replicationFactor());

        Map<String, String> configs = createdTopic.configs();
        assertEquals("604800000", configs.get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals(TopicConfig.CLEANUP_POLICY_DELETE, configs.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("gzip", configs.get(TopicConfig.COMPRESSION_TYPE_CONFIG));
    }

    @Test
    void testEnsureDLQTopicExists_TopicAlreadyExists() throws Exception {
        // Given: Topic already exists
        KafkaFuture<Void> failureFuture = KafkaFuture.failedFuture(
                new TopicExistsException("Topic already exists")
        );
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(failureFuture);

        // When: Ensure topic exists
        CompletableFuture<Void> result = topicManager.ensureDLQTopicExists("orders");

        // Then: Should complete successfully (topic exists is OK)
        assertDoesNotThrow(() -> result.get());
        assertTrue(topicManager.isDLQTopicCreated("orders"),
                "Topic should be marked as created even if already existed");
    }

    @Test
    void testEnsureDLQTopicExists_CreationFails() {
        // Given: Topic creation fails
        KafkaFuture<Void> failureFuture = KafkaFuture.failedFuture(
                new RuntimeException("Broker unavailable")
        );
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(failureFuture);

        // When: Ensure topic exists
        CompletableFuture<Void> result = topicManager.ensureDLQTopicExists("orders");

        // Then: Should complete exceptionally
        assertThrows(ExecutionException.class, () -> result.get());
        assertFalse(topicManager.isDLQTopicCreated("orders"),
                "Topic should be marked as failed in cache");
    }

    // ========== Caching Tests ==========

    @Test
    void testCaching_SecondCallUsesCache() throws Exception {
        // Given: First call succeeds
        KafkaFuture<Void> successFuture = KafkaFuture.completedFuture(null);
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(successFuture);

        // When: Call twice for same topic
        topicManager.ensureDLQTopicExists("orders").get();
        topicManager.ensureDLQTopicExists("orders").get();

        // Then: Admin client called only once (second call used cache)
        verify(adminClient, times(1)).createTopics(any());
    }

    @Test
    void testCaching_FailureAlsoCached() throws Exception {
        // Given: First call fails
        KafkaFuture<Void> failureFuture = KafkaFuture.failedFuture(
                new RuntimeException("Creation failed")
        );
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(failureFuture);

        // When: Call twice for same topic
        assertThrows(ExecutionException.class,
                () -> topicManager.ensureDLQTopicExists("orders").get());

        CompletableFuture<Void> secondCall = topicManager.ensureDLQTopicExists("orders");

        // Then: Second call should fail immediately (cached failure)
        assertThrows(ExecutionException.class, () -> secondCall.get());
        verify(adminClient, times(1)).createTopics(any());
    }

    @Test
    void testCaching_DifferentTopicsNotCached() throws Exception {
        // Given: Admin client succeeds
        KafkaFuture<Void> successFuture = KafkaFuture.completedFuture(null);
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(successFuture);

        // When: Call for different topics
        topicManager.ensureDLQTopicExists("orders").get();
        topicManager.ensureDLQTopicExists("payments").get();

        // Then: Admin client called twice (different topics)
        verify(adminClient, times(2)).createTopics(any());
    }

    @Test
    void testInvalidateCache() throws Exception {
        // Given: Topic created and cached
        KafkaFuture<Void> successFuture = KafkaFuture.completedFuture(null);
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(successFuture);

        topicManager.ensureDLQTopicExists("orders").get();
        assertTrue(topicManager.isDLQTopicCreated("orders"));

        // When: Invalidate cache
        topicManager.invalidateCache("orders");

        // Then: Cache entry removed
        assertFalse(topicManager.isDLQTopicCreated("orders"));

        // And: Next call creates topic again
        topicManager.ensureDLQTopicExists("orders").get();
        verify(adminClient, times(2)).createTopics(any());
    }

    @Test
    void testClearCache() throws Exception {
        // Given: Multiple topics cached
        KafkaFuture<Void> successFuture = KafkaFuture.completedFuture(null);
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(successFuture);

        topicManager.ensureDLQTopicExists("orders").get();
        topicManager.ensureDLQTopicExists("payments").get();
        assertEquals(2, topicManager.cacheSize());

        // When: Clear cache
        topicManager.clearCache();

        // Then: All entries removed
        assertEquals(0, topicManager.cacheSize());
    }

    // ========== Auto-Create Disabled Tests ==========

    @Test
    void testAutoCreateDisabled() {
        // Given: Config with auto-create disabled
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, false);
        props.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 5);
        props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 30000);
        props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, true);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_CONFIG, false);

        AbstractConfig abstractConfig = new AbstractConfig(ShareGroupConfig.CONFIG_DEF, props);
        ShareGroupConfig configNoAuto = new ShareGroupConfig(abstractConfig);
        DLQTopicManager managerNoAuto = new DLQTopicManager(adminClient, configNoAuto);

        // When: Ensure topic exists
        CompletableFuture<Void> result = managerNoAuto.ensureDLQTopicExists("orders");

        // Then: Should fail immediately
        assertThrows(ExecutionException.class, () -> result.get());

        // And: No admin client call
        verify(adminClient, never()).createTopics(any());

        // And: Failure cached
        assertFalse(managerNoAuto.isDLQTopicCreated("orders"));
    }

    // ========== Concurrent Creation Tests ==========

    @Test
    void testConcurrentCreationForSameTopic() throws Exception {
        // Given: Slow topic creation
        CompletableFuture<Void> slowCreation = new CompletableFuture<>();
        KafkaFuture<Void> kafkaFuture = KafkaFuture.from(slowCreation);
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(kafkaFuture);

        // When: Multiple concurrent calls
        CompletableFuture<Void> call1 = topicManager.ensureDLQTopicExists("orders");
        CompletableFuture<Void> call2 = topicManager.ensureDLQTopicExists("orders");

        // Complete the creation
        slowCreation.complete(null);

        // Then: Both calls complete
        assertDoesNotThrow(() -> call1.get());
        assertDoesNotThrow(() -> call2.get());

        // And: Topic created only once
        verify(adminClient, times(1)).createTopics(any());
    }

    // ========== Edge Cases ==========

    @Test
    void testEmptySourceTopic() {
        // Given: Empty source topic
        String emptyTopic = "";

        // When: Build DLQ name
        String dlqTopic = topicManager.buildDLQTopicName(emptyTopic);

        // Then: Should still work (prefix only)
        assertEquals("__test_dlq_", dlqTopic);
    }

    @Test
    void testVeryLongTopicName() {
        // Given: Very long topic name (Kafka limit is 249 characters)
        String longTopic = "a".repeat(200);

        // When: Build DLQ name
        String dlqTopic = topicManager.buildDLQTopicName(longTopic);

        // Then: Should concatenate (may exceed Kafka limit - that's broker's concern)
        assertTrue(dlqTopic.startsWith("__test_dlq_"));
        assertTrue(dlqTopic.length() > 200);
    }

    @Test
    void testTopicWithDots() {
        // Given: Topic with dots (common pattern)
        String dottedTopic = "events.v1.production";

        // When: Build DLQ name and create
        String dlqTopic = topicManager.buildDLQTopicName(dottedTopic);

        // Then: Dots preserved
        assertEquals("__test_dlq_events.v1.production", dlqTopic);
    }

    // ========== Resource Management Tests ==========

    @Test
    void testClose() {
        // Given: Topic manager with admin client
        // When: Close
        topicManager.close();

        // Then: Admin client closed
        verify(adminClient, times(1)).close();
    }

    @Test
    void testCloseWithException() {
        // Given: Admin client throws on close
        doThrow(new RuntimeException("Close error")).when(adminClient).close();

        // When/Then: Should not throw (error logged only)
        assertDoesNotThrow(() -> topicManager.close());
    }
}
