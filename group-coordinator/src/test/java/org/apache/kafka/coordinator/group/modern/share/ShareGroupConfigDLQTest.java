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
package org.apache.kafka.coordinator.group.modern.share;

import org.apache.kafka.common.config.AbstractConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ShareGroupConfig DLQ functionality.
 *
 * Tests cover:
 * - Default values
 * - Configuration validation
 * - Getter methods
 * - Edge cases and error conditions
 */
public class ShareGroupConfigDLQTest {

    private ShareGroupConfig createConfig(Map<String, Object> overrides) {
        Map<String, Object> props = new HashMap<>();

        // Add minimal required configs for ShareGroupConfig
        props.put(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG, false);
        props.put(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, 5);
        props.put(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, 30000);
        props.put(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, 15000);
        props.put(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, 60000);

        // Add overrides
        props.putAll(overrides);

        AbstractConfig config = new AbstractConfig(ShareGroupConfig.CONFIG_DEF, props);
        return new ShareGroupConfig(config);
    }

    // ========== Default Value Tests ==========

    @Test
    void testDLQEnabledDefaultValue() {
        // Given: No DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of());

        // When/Then: Verify DLQ disabled by default
        assertFalse(config.shareGroupDlqEnabled(),
                "DLQ should be disabled by default for backward compatibility");
    }

    @Test
    void testDLQTopicPrefixDefaultValue() {
        // Given: No DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of());

        // When/Then: Verify default prefix
        assertEquals("__share_group_dlq_", config.shareGroupDlqTopicPrefix(),
                "Default DLQ topic prefix should be __share_group_dlq_");
    }

    @Test
    void testDLQReplicationFactorDefaultValue() {
        // Given: No DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of());

        // When/Then: Verify default replication factor
        assertEquals((short) 3, config.shareGroupDlqReplicationFactor(),
                "Default DLQ replication factor should be 3");
    }

    @Test
    void testDLQNumPartitionsDefaultValue() {
        // Given: No DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of());

        // When/Then: Verify default partition count
        assertEquals(1, config.shareGroupDlqNumPartitions(),
                "Default DLQ partition count should be 1 to preserve ordering");
    }

    @Test
    void testDLQIncludeHeadersDefaultValue() {
        // Given: No DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of());

        // When/Then: Verify headers included by default
        assertTrue(config.shareGroupDlqIncludeHeaders(),
                "DLQ should include metadata headers by default");
    }

    @Test
    void testDLQRetentionMsDefaultValue() {
        // Given: No DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of());

        // When/Then: Verify default retention (7 days)
        assertEquals(604800000L, config.shareGroupDlqRetentionMs(),
                "Default DLQ retention should be 7 days (604800000ms)");
    }

    @Test
    void testDLQCompressionTypeDefaultValue() {
        // Given: No DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of());

        // When/Then: Verify default compression
        assertEquals("producer", config.shareGroupDlqCompressionType(),
                "Default DLQ compression type should be 'producer'");
    }

    @Test
    void testDLQAutoCreateTopicsDefaultValue() {
        // Given: No DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of());

        // When/Then: Verify auto-create enabled by default
        assertTrue(config.shareGroupDlqAutoCreateTopics(),
                "DLQ auto-create topics should be enabled by default");
    }

    // ========== Configuration Override Tests ==========

    @Test
    void testEnableDLQ() {
        // Given: DLQ enabled config
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, true
        ));

        // When/Then: Verify DLQ enabled
        assertTrue(config.shareGroupDlqEnabled());
    }

    @Test
    void testCustomDLQTopicPrefix() {
        // Given: Custom prefix
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG, "my_dlq_"
        ));

        // When/Then: Verify custom prefix
        assertEquals("my_dlq_", config.shareGroupDlqTopicPrefix());
    }

    @Test
    void testCustomReplicationFactor() {
        // Given: Custom replication factor
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG, (short) 5
        ));

        // When/Then: Verify custom value
        assertEquals((short) 5, config.shareGroupDlqReplicationFactor());
    }

    @Test
    void testCustomNumPartitions() {
        // Given: Multiple partitions
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG, 10
        ));

        // When/Then: Verify custom value
        assertEquals(10, config.shareGroupDlqNumPartitions());
    }

    @Test
    void testDisableMetadataHeaders() {
        // Given: Headers disabled
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_INCLUDE_HEADERS_CONFIG, false
        ));

        // When/Then: Verify headers disabled
        assertFalse(config.shareGroupDlqIncludeHeaders());
    }

    @Test
    void testCustomRetention() {
        // Given: Custom retention (30 days)
        long thirtyDays = 30L * 24 * 60 * 60 * 1000;
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_RETENTION_MS_CONFIG, thirtyDays
        ));

        // When/Then: Verify custom retention
        assertEquals(thirtyDays, config.shareGroupDlqRetentionMs());
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "lz4", "zstd", "producer"})
    void testValidCompressionTypes(String compressionType) {
        // Given: Valid compression type
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG, compressionType
        ));

        // When/Then: Verify accepted
        assertEquals(compressionType, config.shareGroupDlqCompressionType());
    }

    @Test
    void testDisableAutoCreateTopics() {
        // Given: Auto-create disabled
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_CONFIG, false
        ));

        // When/Then: Verify disabled
        assertFalse(config.shareGroupDlqAutoCreateTopics());
    }

    // ========== Validation Tests ==========

    @Test
    void testEmptyPrefixRejectedWhenDLQEnabled() {
        // Given: DLQ enabled with empty prefix
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, true);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG, "");

        // When/Then: Expect validation failure
        assertThrows(IllegalArgumentException.class, () -> createConfig(props),
                "Empty DLQ topic prefix should be rejected when DLQ is enabled");
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "GZIP", "bzip2", ""})
    void testInvalidCompressionTypeRejected(String invalidType) {
        // Given: DLQ enabled with invalid compression
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, true);
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG, invalidType);

        // When/Then: Expect validation failure
        assertThrows(IllegalArgumentException.class, () -> createConfig(props),
                "Invalid compression type '" + invalidType + "' should be rejected");
    }

    @Test
    void testZeroReplicationFactorRejected() {
        // Given: Replication factor of 0
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG, (short) 0);

        // When/Then: Expect config validation failure
        assertThrows(Exception.class, () -> createConfig(props),
                "Replication factor must be at least 1");
    }

    @Test
    void testZeroPartitionsRejected() {
        // Given: 0 partitions
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG, 0);

        // When/Then: Expect config validation failure
        assertThrows(Exception.class, () -> createConfig(props),
                "Number of partitions must be at least 1");
    }

    @Test
    void testNegativeRetentionRejected() {
        // Given: Negative retention
        Map<String, Object> props = new HashMap<>();
        props.put(ShareGroupConfig.SHARE_GROUP_DLQ_RETENTION_MS_CONFIG, -1L);

        // When/Then: Expect config validation failure
        assertThrows(Exception.class, () -> createConfig(props),
                "Retention must be positive");
    }

    // ========== Integration Tests ==========

    @Test
    void testAllDLQConfigsTogether() {
        // Given: All DLQ configs specified
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, true,
                ShareGroupConfig.SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG, "test_dlq_",
                ShareGroupConfig.SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG, (short) 2,
                ShareGroupConfig.SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG, 3,
                ShareGroupConfig.SHARE_GROUP_DLQ_INCLUDE_HEADERS_CONFIG, false,
                ShareGroupConfig.SHARE_GROUP_DLQ_RETENTION_MS_CONFIG, 86400000L, // 1 day
                ShareGroupConfig.SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG, "gzip",
                ShareGroupConfig.SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_CONFIG, false
        ));

        // When/Then: Verify all values
        assertTrue(config.shareGroupDlqEnabled());
        assertEquals("test_dlq_", config.shareGroupDlqTopicPrefix());
        assertEquals((short) 2, config.shareGroupDlqReplicationFactor());
        assertEquals(3, config.shareGroupDlqNumPartitions());
        assertFalse(config.shareGroupDlqIncludeHeaders());
        assertEquals(86400000L, config.shareGroupDlqRetentionMs());
        assertEquals("gzip", config.shareGroupDlqCompressionType());
        assertFalse(config.shareGroupDlqAutoCreateTopics());
    }

    @Test
    void testDLQDisabledIgnoresOtherConfigs() {
        // Given: DLQ disabled but other configs set
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG, false,
                ShareGroupConfig.SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG, "test_",
                ShareGroupConfig.SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG, (short) 5
        ));

        // When: DLQ disabled
        // Then: Other configs still accessible but DLQ disabled
        assertFalse(config.shareGroupDlqEnabled(),
                "DLQ should be disabled regardless of other configs");
        assertEquals("test_", config.shareGroupDlqTopicPrefix(),
                "Other configs should still be accessible");
    }

    // ========== Edge Case Tests ==========

    @Test
    void testVeryLargeRetention() {
        // Given: Very large retention (1 year)
        long oneYear = 365L * 24 * 60 * 60 * 1000;
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_RETENTION_MS_CONFIG, oneYear
        ));

        // When/Then: Should be accepted
        assertEquals(oneYear, config.shareGroupDlqRetentionMs());
    }

    @Test
    void testVeryLargePartitionCount() {
        // Given: Large partition count
        ShareGroupConfig config = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG, 1000
        ));

        // When/Then: Should be accepted
        assertEquals(1000, config.shareGroupDlqNumPartitions());
    }

    @Test
    void testCompressionTypeCaseInsensitive() {
        // Given: Compression type in different cases
        ShareGroupConfig config1 = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG, "GZIP"
        ));
        ShareGroupConfig config2 = createConfig(Map.of(
                ShareGroupConfig.SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG, "GZip"
        ));

        // When/Then: Both should work (validation is case-insensitive)
        // Note: The actual value is stored as provided
        assertEquals("GZIP", config1.shareGroupDlqCompressionType());
        assertEquals("GZip", config2.shareGroupDlqCompressionType());
    }
}
