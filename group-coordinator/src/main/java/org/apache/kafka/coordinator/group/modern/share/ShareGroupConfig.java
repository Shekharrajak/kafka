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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class ShareGroupConfig {
    /** Share Group Configurations **/

    // Internal configuration used by integration and system tests.
    public static final String SHARE_GROUP_ENABLE_CONFIG = "group.share.enable";
    public static final boolean SHARE_GROUP_ENABLE_DEFAULT = false;
    public static final String SHARE_GROUP_ENABLE_DOC = "Enable share groups on the broker.";

    public static final String SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG = "group.share.partition.max.record.locks";
    public static final int SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT = 2000;
    public static final String SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DOC = "Share-group record lock limit per share-partition.";

    public static final String SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG = "group.share.delivery.count.limit";
    public static final int SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT = 5;
    public static final String SHARE_GROUP_DELIVERY_COUNT_LIMIT_DOC = "The maximum number of delivery attempts for a record delivered to a share group.";

    public static final String SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.record.lock.duration.ms";
    public static final int SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT = 30000;
    public static final String SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC = "The record acquisition lock duration in milliseconds for share groups.";

    public static final String SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.min.record.lock.duration.ms";
    public static final int SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT = 15000;
    public static final String SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DOC = "The record acquisition lock minimum duration in milliseconds for share groups.";

    public static final String SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG = "group.share.max.record.lock.duration.ms";
    public static final int SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT = 60000;
    public static final String SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DOC = "The record acquisition lock maximum duration in milliseconds for share groups.";

    public static final String SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG = "share.fetch.purgatory.purge.interval.requests";
    public static final int SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1000;
    public static final String SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the share fetch request purgatory";

    public static final String SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG = "group.share.max.share.sessions";
    public static final int SHARE_GROUP_MAX_SHARE_SESSIONS_DEFAULT = 2000;
    public static final String SHARE_GROUP_MAX_SHARE_SESSIONS_DOC = "The maximum number of share sessions per broker.";

    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG = "group.share.persister.class.name";
    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_DEFAULT = "org.apache.kafka.server.share.persister.DefaultStatePersister";
    public static final String SHARE_GROUP_PERSISTER_CLASS_NAME_DOC = "The fully qualified name of a class which implements " +
        "the <code>org.apache.kafka.server.share.Persister</code> interface.";

    /** Dead Letter Queue (DLQ) Configurations **/

    public static final String SHARE_GROUP_DLQ_ENABLED_CONFIG = "group.share.dlq.enabled";
    public static final boolean SHARE_GROUP_DLQ_ENABLED_DEFAULT = false;
    public static final String SHARE_GROUP_DLQ_ENABLED_DOC = "Enable Dead Letter Queue (DLQ) for rejected records in share groups. " +
        "When enabled, records acknowledged with REJECT are written to a DLQ topic with metadata headers.";

    public static final String SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG = "group.share.dlq.topic.prefix";
    public static final String SHARE_GROUP_DLQ_TOPIC_PREFIX_DEFAULT = "__share_group_dlq_";
    public static final String SHARE_GROUP_DLQ_TOPIC_PREFIX_DOC = "Prefix for auto-created DLQ topic names. " +
        "DLQ topic name format: {prefix}{source_topic}. For example, with default prefix '__share_group_dlq_' " +
        "and source topic 'orders', the DLQ topic will be '__share_group_dlq_orders'.";

    public static final String SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG = "group.share.dlq.replication.factor";
    public static final short SHARE_GROUP_DLQ_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String SHARE_GROUP_DLQ_REPLICATION_FACTOR_DOC = "Replication factor for auto-created DLQ topics. " +
        "This value must not exceed the number of brokers in the cluster.";

    public static final String SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG = "group.share.dlq.num.partitions";
    public static final int SHARE_GROUP_DLQ_NUM_PARTITIONS_DEFAULT = 1;
    public static final String SHARE_GROUP_DLQ_NUM_PARTITIONS_DOC = "Number of partitions for auto-created DLQ topics. " +
        "Using a single partition (default) preserves ordering of rejected records from the same source partition.";

    public static final String SHARE_GROUP_DLQ_INCLUDE_HEADERS_CONFIG = "group.share.dlq.include.headers";
    public static final boolean SHARE_GROUP_DLQ_INCLUDE_HEADERS_DEFAULT = true;
    public static final String SHARE_GROUP_DLQ_INCLUDE_HEADERS_DOC = "Include metadata headers in DLQ records. " +
        "Headers include: original topic, partition, offset, group ID, delivery count, rejection timestamp, member ID, and optional failure reason.";

    public static final String SHARE_GROUP_DLQ_RETENTION_MS_CONFIG = "group.share.dlq.retention.ms";
    public static final long SHARE_GROUP_DLQ_RETENTION_MS_DEFAULT = 604800000L; // 7 days
    public static final String SHARE_GROUP_DLQ_RETENTION_MS_DOC = "Retention time in milliseconds for DLQ topics. " +
        "Default is 7 days (604800000ms). After this period, DLQ records are eligible for deletion.";

    public static final String SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG = "group.share.dlq.compression.type";
    public static final String SHARE_GROUP_DLQ_COMPRESSION_TYPE_DEFAULT = "producer";
    public static final String SHARE_GROUP_DLQ_COMPRESSION_TYPE_DOC = "Compression type for DLQ records. " +
        "Valid values: 'none', 'gzip', 'snappy', 'lz4', 'zstd', or 'producer' (use original record's compression).";

    public static final String SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_CONFIG = "group.share.dlq.auto.create.topics";
    public static final boolean SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_DEFAULT = true;
    public static final String SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_DOC = "Automatically create DLQ topics on first record rejection. " +
        "If disabled, DLQ topics must be created manually before records can be routed to DLQ.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .defineInternal(SHARE_GROUP_ENABLE_CONFIG, BOOLEAN, SHARE_GROUP_ENABLE_DEFAULT, null, MEDIUM, SHARE_GROUP_ENABLE_DOC)
            .define(SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG, INT, SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT, between(2, 10), MEDIUM, SHARE_GROUP_DELIVERY_COUNT_LIMIT_DOC)
            .define(SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT, between(1000, 3600000), MEDIUM, SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DEFAULT, between(1000, 30000), MEDIUM, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, INT, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DEFAULT, between(30000, 3600000), MEDIUM, SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_DOC)
            .define(SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG, INT, SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DEFAULT, between(100, 10000), MEDIUM, SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_DOC)
            .define(SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
            .define(SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG, INT, SHARE_GROUP_MAX_SHARE_SESSIONS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_MAX_SHARE_SESSIONS_DOC)
            .defineInternal(SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG, STRING, SHARE_GROUP_PERSISTER_CLASS_NAME_DEFAULT, null, MEDIUM, SHARE_GROUP_PERSISTER_CLASS_NAME_DOC)
            // DLQ configurations
            .define(SHARE_GROUP_DLQ_ENABLED_CONFIG, BOOLEAN, SHARE_GROUP_DLQ_ENABLED_DEFAULT, MEDIUM, SHARE_GROUP_DLQ_ENABLED_DOC)
            .define(SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG, STRING, SHARE_GROUP_DLQ_TOPIC_PREFIX_DEFAULT, MEDIUM, SHARE_GROUP_DLQ_TOPIC_PREFIX_DOC)
            .define(SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG, ConfigDef.Type.SHORT, SHARE_GROUP_DLQ_REPLICATION_FACTOR_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_DLQ_REPLICATION_FACTOR_DOC)
            .define(SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG, INT, SHARE_GROUP_DLQ_NUM_PARTITIONS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_DLQ_NUM_PARTITIONS_DOC)
            .define(SHARE_GROUP_DLQ_INCLUDE_HEADERS_CONFIG, BOOLEAN, SHARE_GROUP_DLQ_INCLUDE_HEADERS_DEFAULT, MEDIUM, SHARE_GROUP_DLQ_INCLUDE_HEADERS_DOC)
            .define(SHARE_GROUP_DLQ_RETENTION_MS_CONFIG, ConfigDef.Type.LONG, SHARE_GROUP_DLQ_RETENTION_MS_DEFAULT, atLeast(1L), MEDIUM, SHARE_GROUP_DLQ_RETENTION_MS_DOC)
            .define(SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG, STRING, SHARE_GROUP_DLQ_COMPRESSION_TYPE_DEFAULT, MEDIUM, SHARE_GROUP_DLQ_COMPRESSION_TYPE_DOC)
            .define(SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_CONFIG, BOOLEAN, SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_DEFAULT, MEDIUM, SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_DOC);

    private final boolean isShareGroupEnabled;
    private final int shareGroupPartitionMaxRecordLocks;
    private final int shareGroupDeliveryCountLimit;
    private final int shareGroupRecordLockDurationMs;
    private final int shareGroupMaxRecordLockDurationMs;
    private final int shareGroupMinRecordLockDurationMs;
    private final int shareFetchPurgatoryPurgeIntervalRequests;
    private final int shareGroupMaxShareSessions;
    private final String shareGroupPersisterClassName;

    // DLQ fields
    private final boolean shareGroupDlqEnabled;
    private final String shareGroupDlqTopicPrefix;
    private final short shareGroupDlqReplicationFactor;
    private final int shareGroupDlqNumPartitions;
    private final boolean shareGroupDlqIncludeHeaders;
    private final long shareGroupDlqRetentionMs;
    private final String shareGroupDlqCompressionType;
    private final boolean shareGroupDlqAutoCreateTopics;

    private final AbstractConfig config;

    public ShareGroupConfig(AbstractConfig config) {
        this.config = config;
        // The proper way to enable share groups is to use the share.version feature with v1 or later.
        isShareGroupEnabled = config.getBoolean(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG);
        shareGroupPartitionMaxRecordLocks = config.getInt(ShareGroupConfig.SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS_CONFIG);
        shareGroupDeliveryCountLimit = config.getInt(ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_CONFIG);
        shareGroupRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG);
        shareGroupMaxRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG);
        shareGroupMinRecordLockDurationMs = config.getInt(ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
        shareFetchPurgatoryPurgeIntervalRequests = config.getInt(ShareGroupConfig.SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG);
        shareGroupMaxShareSessions = config.getInt(ShareGroupConfig.SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG);
        shareGroupPersisterClassName = config.getString(ShareGroupConfig.SHARE_GROUP_PERSISTER_CLASS_NAME_CONFIG);

        // DLQ configurations
        shareGroupDlqEnabled = config.getBoolean(ShareGroupConfig.SHARE_GROUP_DLQ_ENABLED_CONFIG);
        shareGroupDlqTopicPrefix = config.getString(ShareGroupConfig.SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG);
        shareGroupDlqReplicationFactor = config.getShort(ShareGroupConfig.SHARE_GROUP_DLQ_REPLICATION_FACTOR_CONFIG);
        shareGroupDlqNumPartitions = config.getInt(ShareGroupConfig.SHARE_GROUP_DLQ_NUM_PARTITIONS_CONFIG);
        shareGroupDlqIncludeHeaders = config.getBoolean(ShareGroupConfig.SHARE_GROUP_DLQ_INCLUDE_HEADERS_CONFIG);
        shareGroupDlqRetentionMs = config.getLong(ShareGroupConfig.SHARE_GROUP_DLQ_RETENTION_MS_CONFIG);
        shareGroupDlqCompressionType = config.getString(ShareGroupConfig.SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG);
        shareGroupDlqAutoCreateTopics = config.getBoolean(ShareGroupConfig.SHARE_GROUP_DLQ_AUTO_CREATE_TOPICS_CONFIG);

        validate();
    }

    /** Share group configuration **/
    public boolean isShareGroupEnabled() {
        return isShareGroupEnabled;
    }

    public int shareGroupPartitionMaxRecordLocks() {
        return shareGroupPartitionMaxRecordLocks;
    }

    public int shareGroupDeliveryCountLimit() {
        return shareGroupDeliveryCountLimit;
    }

    public int shareGroupRecordLockDurationMs() {
        return shareGroupRecordLockDurationMs;
    }

    public int shareGroupMaxRecordLockDurationMs() {
        return shareGroupMaxRecordLockDurationMs;
    }

    public int shareGroupMinRecordLockDurationMs() {
        return shareGroupMinRecordLockDurationMs;
    }

    public int shareFetchPurgatoryPurgeIntervalRequests() {
        return shareFetchPurgatoryPurgeIntervalRequests;
    }

    public int shareGroupMaxShareSessions() {
        return shareGroupMaxShareSessions;
    }

    public String shareGroupPersisterClassName() {
        return shareGroupPersisterClassName;
    }

    /** Dead Letter Queue (DLQ) configuration **/

    public boolean shareGroupDlqEnabled() {
        return shareGroupDlqEnabled;
    }

    public String shareGroupDlqTopicPrefix() {
        return shareGroupDlqTopicPrefix;
    }

    public short shareGroupDlqReplicationFactor() {
        return shareGroupDlqReplicationFactor;
    }

    public int shareGroupDlqNumPartitions() {
        return shareGroupDlqNumPartitions;
    }

    public boolean shareGroupDlqIncludeHeaders() {
        return shareGroupDlqIncludeHeaders;
    }

    public long shareGroupDlqRetentionMs() {
        return shareGroupDlqRetentionMs;
    }

    public String shareGroupDlqCompressionType() {
        return shareGroupDlqCompressionType;
    }

    public boolean shareGroupDlqAutoCreateTopics() {
        return shareGroupDlqAutoCreateTopics;
    }

    private void validate() {
        Utils.require(shareGroupRecordLockDurationMs >= shareGroupMinRecordLockDurationMs,
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG));
        Utils.require(shareGroupMaxRecordLockDurationMs >= shareGroupRecordLockDurationMs,
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG));
        Utils.require(shareGroupMaxShareSessions >= config.getInt(GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG),
                String.format("%s must be greater than or equal to %s",
                        SHARE_GROUP_MAX_SHARE_SESSIONS_CONFIG, GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG));

        // DLQ validation
        if (shareGroupDlqEnabled) {
            Utils.require(shareGroupDlqTopicPrefix != null && !shareGroupDlqTopicPrefix.isEmpty(),
                    String.format("%s cannot be empty when DLQ is enabled", SHARE_GROUP_DLQ_TOPIC_PREFIX_CONFIG));

            // Validate compression type
            String compressionType = shareGroupDlqCompressionType.toLowerCase(Locale.ROOT);
            Utils.require(
                    compressionType.equals("none") ||
                    compressionType.equals("gzip") ||
                    compressionType.equals("snappy") ||
                    compressionType.equals("lz4") ||
                    compressionType.equals("zstd") ||
                    compressionType.equals("producer"),
                    String.format("%s must be one of: none, gzip, snappy, lz4, zstd, or producer",
                            SHARE_GROUP_DLQ_COMPRESSION_TYPE_CONFIG));
        }
    }

    /**
     * Copy the subset of properties that are relevant to share group. These configs include those which can be set
     * statically (for all groups) or dynamically (for a specific group). In those cases, the default value for the
     * group specific dynamic config (Ex. share.session.timeout.ms) should be the value set for the static config
     * (Ex. group.share.session.timeout.ms).
     */
    public Map<String, Integer> extractShareGroupConfigMap(GroupCoordinatorConfig groupCoordinatorConfig) {
        return Map.of(
            GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG, groupCoordinatorConfig.shareGroupSessionTimeoutMs(),
            GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, groupCoordinatorConfig.shareGroupHeartbeatIntervalMs(),
            GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, shareGroupRecordLockDurationMs()
        );
    }
}
