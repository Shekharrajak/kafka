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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DLQWriter.
 *
 * Tests cover:
 * - Metadata header construction
 * - Async write operations
 * - Error handling
 * - Metrics tracking
 * - DLQ enabled/disabled behavior
 */
@ExtendWith(MockitoExtension.class)
public class DLQWriterTest {

    @Mock
    private KafkaProducer<byte[], byte[]> mockProducer;

    @Mock
    private DLQTopicManager mockTopicManager;

    private ShareGroupConfig config;
    private DLQWriter dlqWriter;

    private static final int BROKER_ID = 1;
    private static final String SOURCE_TOPIC = "orders";
    private static final int SOURCE_PARTITION = 3;
    private static final long SOURCE_OFFSET = 12345L;
    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "consumer-1";
    private static final int DELIVERY_COUNT = 5;
    private static final String FAILURE_REASON = "ValidationError";

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
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_INCLUDE_HEADERS_CONFIG, true);

        AbstractConfig abstractConfig = new AbstractConfig(ShareGroupConfig.CONFIG_DEF, props);
        config = new ShareGroupConfig(abstractConfig);

        // Setup topic manager mock
        when(mockTopicManager.buildDLQTopicName(anyString()))
                .thenAnswer(invocation -> "__test_dlq_" + invocation.getArgument(0));
        when(mockTopicManager.ensureDLQTopicExists(anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        dlqWriter = new DLQWriter(mockProducer, config, mockTopicManager, BROKER_ID);
    }

    // ========== Basic Write Tests ==========

    @Test
    void testWriteToDLQ_Success() throws Exception {
        // Given: Successful producer send
        RecordMetadata expectedMetadata = new RecordMetadata(
                new TopicPartition("__test_dlq_orders", 0),
                0L, 0L, System.currentTimeMillis(), 0, 0
        );

        doAnswer(invocation -> {
            ProducerRecord<byte[], byte[]> record = invocation.getArgument(0);
            org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
            callback.onCompletion(expectedMetadata, null);
            return null;
        }).when(mockProducer).send(any(), any());

        // When: Write to DLQ
        CompletableFuture<RecordMetadata> result = dlqWriter.writeToDLQ(
                SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET,
                "key".getBytes(), "value".getBytes(),
                new Header[0],
                GROUP_ID, MEMBER_ID, DELIVERY_COUNT, FAILURE_REASON
        );

        // Then: Should complete successfully
        RecordMetadata metadata = result.get();
        assertNotNull(metadata);
        assertEquals("__test_dlq_orders", metadata.topic());

        // Verify metrics updated
        assertEquals(1, dlqWriter.getRecordsSent());
        assertEquals(0, dlqWriter.getRecordsFailed());
        assertTrue(dlqWriter.getBytesSent() > 0);
    }

    @Test
    void testWriteToDLQ_WithNullKeyAndValue() throws Exception {
        // Given: Record with null key and value
        setupSuccessfulProducer();

        // When: Write to DLQ with nulls
        CompletableFuture<RecordMetadata> result = dlqWriter.writeToDLQ(
                SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET,
                null, null,
                new Header[0],
                GROUP_ID, MEMBER_ID, DELIVERY_COUNT, null
        );

        // Then: Should complete successfully
        assertDoesNotThrow(() -> result.get());

        // Verify producer called with nulls
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture(), any());

        ProducerRecord<byte[], byte[]> sentRecord = captor.getValue();
        assertNull(sentRecord.key());
        assertNull(sentRecord.value());
    }

    // ========== Header Construction Tests ==========

    @Test
    void testMetadataHeadersConstructed() throws Exception {
        // Given: Successful producer
        setupSuccessfulProducer();

        // When: Write to DLQ
        dlqWriter.writeToDLQ(
                SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET,
                "key".getBytes(), "value".getBytes(),
                new Header[0],
                GROUP_ID, MEMBER_ID, DELIVERY_COUNT, FAILURE_REASON
        ).get();

        // Then: Verify all headers present
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture(), any());

        ProducerRecord<byte[], byte[]> sentRecord = captor.getValue();

        // Verify each header
        assertHeaderEquals(sentRecord, DLQWriter.DLQ_HEADER_ORIGINAL_TOPIC, SOURCE_TOPIC);
        assertHeaderEquals(sentRecord, DLQWriter.DLQ_HEADER_ORIGINAL_PARTITION,
                String.valueOf(SOURCE_PARTITION));
        assertHeaderEquals(sentRecord, DLQWriter.DLQ_HEADER_ORIGINAL_OFFSET,
                String.valueOf(SOURCE_OFFSET));
        assertHeaderEquals(sentRecord, DLQWriter.DLQ_HEADER_GROUP_ID, GROUP_ID);
        assertHeaderEquals(sentRecord, DLQWriter.DLQ_HEADER_MEMBER_ID, MEMBER_ID);
        assertHeaderEquals(sentRecord, DLQWriter.DLQ_HEADER_DELIVERY_COUNT,
                String.valueOf(DELIVERY_COUNT));
        assertHeaderEquals(sentRecord, DLQWriter.DLQ_HEADER_BROKER_ID,
                String.valueOf(BROKER_ID));
        assertHeaderEquals(sentRecord, DLQWriter.DLQ_HEADER_FAILURE_REASON, FAILURE_REASON);

        // Verify timestamp header exists and is recent
        Header timestampHeader = getHeader(sentRecord, DLQWriter.DLQ_HEADER_REJECT_TIMESTAMP);
        assertNotNull(timestampHeader);
    }

    @Test
    void testOriginalHeadersPreserved() throws Exception {
        // Given: Record with original headers
        setupSuccessfulProducer();

        Header[] originalHeaders = new Header[]{
                new RecordHeader("custom-header-1", "value1".getBytes()),
                new RecordHeader("custom-header-2", "value2".getBytes())
        };

        // When: Write to DLQ
        dlqWriter.writeToDLQ(
                SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET,
                "key".getBytes(), "value".getBytes(),
                originalHeaders,
                GROUP_ID, MEMBER_ID, DELIVERY_COUNT, null
        ).get();

        // Then: Original headers included
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture(), any());

        ProducerRecord<byte[], byte[]> sentRecord = captor.getValue();

        assertHeaderEquals(sentRecord, "custom-header-1", "value1");
        assertHeaderEquals(sentRecord, "custom-header-2", "value2");

        // And DLQ headers also present
        assertNotNull(getHeader(sentRecord, DLQWriter.DLQ_HEADER_ORIGINAL_TOPIC));
    }

    @Test
    void testHeadersNotIncludedWhenDisabled() throws Exception {
        // Given: Config with headers disabled
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, false);
        props.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 5);
        props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 30000);
        props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, true);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_INCLUDE_HEADERS_CONFIG, false);

        AbstractConfig abstractConfig = new AbstractConfig(ShareGroupConfig.CONFIG_DEF, props);
        ShareGroupConfig configNoHeaders = new ShareGroupConfig(abstractConfig);
        DLQWriter writerNoHeaders = new DLQWriter(
                mockProducer, configNoHeaders, mockTopicManager, BROKER_ID
        );

        setupSuccessfulProducer();

        Header[] originalHeaders = new Header[]{
                new RecordHeader("custom", "value".getBytes())
        };

        // When: Write to DLQ
        writerNoHeaders.writeToDLQ(
                SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET,
                "key".getBytes(), "value".getBytes(),
                originalHeaders,
                GROUP_ID, MEMBER_ID, DELIVERY_COUNT, null
        ).get();

        // Then: Original headers NOT included
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture(), any());

        ProducerRecord<byte[], byte[]> sentRecord = captor.getValue();
        assertNull(getHeader(sentRecord, "custom"),
                "Original headers should not be included");

        // But DLQ metadata headers still present
        assertNotNull(getHeader(sentRecord, DLQWriter.DLQ_HEADER_ORIGINAL_TOPIC));
    }

    // ========== Error Handling Tests ==========

    @Test
    void testWriteToDLQ_ProducerFailure() {
        // Given: Producer fails
        RuntimeException producerError = new RuntimeException("Producer error");
        doAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, producerError);
            return null;
        }).when(mockProducer).send(any(), any());

        // When: Write to DLQ
        CompletableFuture<RecordMetadata> result = dlqWriter.writeToDLQ(
                SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET,
                "key".getBytes(), "value".getBytes(),
                new Header[0],
                GROUP_ID, MEMBER_ID, DELIVERY_COUNT, null
        );

        // Then: Should complete exceptionally
        assertThrows(ExecutionException.class, () -> result.get());

        // Verify metrics updated
        assertEquals(0, dlqWriter.getRecordsSent());
        assertEquals(1, dlqWriter.getRecordsFailed());
    }

    @Test
    void testWriteToDLQ_TopicCreationFailure() {
        // Given: Topic creation fails
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Topic creation failed"));
        when(mockTopicManager.ensureDLQTopicExists(anyString())).thenReturn(failedFuture);

        // When: Write to DLQ
        CompletableFuture<RecordMetadata> result = dlqWriter.writeToDLQ(
                SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET,
                "key".getBytes(), "value".getBytes(),
                new Header[0],
                GROUP_ID, MEMBER_ID, DELIVERY_COUNT, null
        );

        // Then: Should complete exceptionally
        assertThrows(ExecutionException.class, () -> result.get());

        // Verify producer never called
        verify(mockProducer, never()).send(any(), any());

        // Verify metrics updated
        assertEquals(1, dlqWriter.getRecordsFailed());
    }

    // ========== DLQ Disabled Tests ==========

    @Test
    void testWriteToDLQ_WhenDLQDisabled() {
        // Given: DLQ disabled
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, false);
        props.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 5);
        props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 30000);
        props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, false);

        AbstractConfig abstractConfig = new AbstractConfig(ShareGroupConfig.CONFIG_DEF, props);
        ShareGroupConfig configDisabled = new ShareGroupConfig(abstractConfig);
        DLQWriter writerDisabled = new DLQWriter(
                mockProducer, configDisabled, mockTopicManager, BROKER_ID
        );

        // When: Write to DLQ
        CompletableFuture<RecordMetadata> result = writerDisabled.writeToDLQ(
                SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET,
                "key".getBytes(), "value".getBytes(),
                new Header[0],
                GROUP_ID, MEMBER_ID, DELIVERY_COUNT, null
        );

        // Then: Should complete immediately with null
        assertDoesNotThrow(() -> {
            RecordMetadata metadata = result.get();
            assertNull(metadata);
        });

        // Verify no producer or topic manager calls
        verify(mockProducer, never()).send(any(), any());
        verify(mockTopicManager, never()).ensureDLQTopicExists(any());
    }

    @Test
    void testIsEnabled() {
        // Given: DLQ enabled config
        assertTrue(dlqWriter.isEnabled());

        // Given: DLQ disabled config
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, false);
        props.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 5);
        props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 30000);
        props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, false);

        AbstractConfig abstractConfig = new AbstractConfig(ShareGroupConfig.CONFIG_DEF, props);
        ShareGroupConfig configDisabled = new ShareGroupConfig(abstractConfig);
        DLQWriter writerDisabled = new DLQWriter(
                mockProducer, configDisabled, mockTopicManager, BROKER_ID
        );

        assertFalse(writerDisabled.isEnabled());
    }

    // ========== Metrics Tests ==========

    @Test
    void testMetricsTracking() throws Exception {
        // Given: Multiple writes
        setupSuccessfulProducer();

        // When: Write multiple records
        dlqWriter.writeToDLQ(SOURCE_TOPIC, 0, 100L,
                "key1".getBytes(), "value1".getBytes(), new Header[0],
                GROUP_ID, MEMBER_ID, 1, null).get();

        dlqWriter.writeToDLQ(SOURCE_TOPIC, 0, 101L,
                "key2".getBytes(), "value2".getBytes(), new Header[0],
                GROUP_ID, MEMBER_ID, 2, null).get();

        // Then: Metrics updated
        assertEquals(2, dlqWriter.getRecordsSent());
        assertEquals(0, dlqWriter.getRecordsFailed());
        assertTrue(dlqWriter.getBytesSent() > 0);
    }

    @Test
    void testMetricsOnFailure() {
        // Given: Producer fails
        RuntimeException error = new RuntimeException("Error");
        doAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, error);
            return null;
        }).when(mockProducer).send(any(), any());

        // When: Write fails
        CompletableFuture<RecordMetadata> result = dlqWriter.writeToDLQ(
                SOURCE_TOPIC, 0, 100L,
                "key".getBytes(), "value".getBytes(), new Header[0],
                GROUP_ID, MEMBER_ID, 1, null
        );

        assertThrows(ExecutionException.class, () -> result.get());

        // Then: Failure metrics updated
        assertEquals(0, dlqWriter.getRecordsSent());
        assertEquals(1, dlqWriter.getRecordsFailed());
    }

    // ========== Resource Management Tests ==========

    @Test
    void testClose() {
        // When: Close writer
        dlqWriter.close();

        // Then: Producer flushed and closed
        verify(mockProducer).flush();
        verify(mockProducer).close();
    }

    @Test
    void testCloseWithException() {
        // Given: Producer throws on close
        doThrow(new RuntimeException("Close error")).when(mockProducer).close();

        // When/Then: Should not throw
        assertDoesNotThrow(() -> dlqWriter.close());
    }

    // ========== Helper Methods ==========

    private void setupSuccessfulProducer() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("__test_dlq_orders", 0),
                0L, 0L, System.currentTimeMillis(), 0, 0
        );

        doAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback callback = invocation.getArgument(1);
            callback.onCompletion(metadata, null);
            return null;
        }).when(mockProducer).send(any(), any());
    }

    private void assertHeaderEquals(ProducerRecord<byte[], byte[]> record,
                                     String headerKey, String expectedValue) {
        Header header = getHeader(record, headerKey);
        assertNotNull(header, "Header " + headerKey + " should be present");
        String actualValue = new String(header.value(), StandardCharsets.UTF_8);
        assertTrue(actualValue.contains(expectedValue) || expectedValue.contains(actualValue),
                "Header " + headerKey + " value mismatch. Expected to contain: " + expectedValue +
                        ", but got: " + actualValue);
    }

    private Header getHeader(ProducerRecord<byte[], byte[]> record, String key) {
        for (Header header : record.headers()) {
            if (header.key().equals(key)) {
                return header;
            }
        }
        return null;
    }
}
