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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Dead Letter Queue (DLQ) topic creation for share groups.
 * This class handles:
 * - Auto-creation of DLQ topics based on source topic names
 * - Caching of created topics to avoid redundant creation attempts
 * - Applying configured replication factor, partitions, and retention settings
 * - Thread-safe topic existence tracking
 *
 * <p>DLQ topics are created with the naming convention: {prefix}{sourceTopic}
 * For example, with default prefix "__share_group_dlq_" and source topic "orders",
 * the DLQ topic will be "__share_group_dlq_orders".</p>
 */
public class DLQTopicManager {
    private static final Logger log = LoggerFactory.getLogger(DLQTopicManager.class);

    private final Admin adminClient;
    private final ShareGroupConfig config;

    // Cache of topics we've already attempted to create (successful or failed)
    // Key: DLQ topic name, Value: true if creation succeeded, false if failed
    private final ConcurrentHashMap<String, Boolean> topicCreationCache;

    /**
     * Creates a new DLQ topic manager.
     *
     * @param adminClient Admin client for topic operations
     * @param config Share group configuration containing DLQ settings
     */
    public DLQTopicManager(Admin adminClient, ShareGroupConfig config) {
        this.adminClient = adminClient;
        this.config = config;
        this.topicCreationCache = new ConcurrentHashMap<>();
    }

    /**
     * Ensures a DLQ topic exists for the given source topic.
     * This method is idempotent and thread-safe.
     *
     * <p>Behavior:</p>
     * <ul>
     *   <li>If topic already created (in cache), returns immediately</li>
     *   <li>If auto-create disabled, logs warning and returns</li>
     *   <li>If creation fails, caches failure and logs error</li>
     *   <li>Subsequent calls for failed topics skip creation attempt</li>
     * </ul>
     *
     * @param sourceTopic The source topic name
     * @return CompletableFuture that completes when topic creation is done (or skipped)
     */
    public CompletableFuture<Void> ensureDLQTopicExists(String sourceTopic) {
        String dlqTopicName = buildDLQTopicName(sourceTopic);

        // Check cache first - avoid redundant creation attempts
        Boolean cachedResult = topicCreationCache.get(dlqTopicName);
        if (cachedResult != null) {
            if (cachedResult) {
                log.trace("DLQ topic {} already exists (cached)", dlqTopicName);
                return CompletableFuture.completedFuture(null);
            } else {
                log.debug("Skipping DLQ topic creation for {} - previous creation failed", dlqTopicName);
                return CompletableFuture.failedFuture(
                        new IllegalStateException("DLQ topic creation previously failed for: " + dlqTopicName)
                );
            }
        }

        // Check if auto-create is enabled
        if (!config.shareGroupDlqAutoCreateTopics()) {
            log.warn("DLQ topic {} does not exist and auto-create is disabled. " +
                    "Records will not be written to DLQ until topic is manually created.", dlqTopicName);
            topicCreationCache.put(dlqTopicName, false);
            return CompletableFuture.failedFuture(
                    new IllegalStateException("DLQ auto-create disabled for: " + dlqTopicName)
            );
        }

        // Attempt to create the topic
        return createDLQTopic(dlqTopicName, sourceTopic);
    }

    /**
     * Creates a DLQ topic with configured settings.
     *
     * @param dlqTopicName The DLQ topic name
     * @param sourceTopic The source topic name (for logging)
     * @return CompletableFuture that completes when creation succeeds or fails
     */
    private CompletableFuture<Void> createDLQTopic(String dlqTopicName, String sourceTopic) {
        NewTopic newTopic = buildDLQTopicConfig(dlqTopicName);

        log.info("Creating DLQ topic {} for source topic {} with {} partitions and replication factor {}",
                dlqTopicName, sourceTopic, newTopic.numPartitions(), newTopic.replicationFactor());

        CreateTopicsResult createResult = adminClient.createTopics(Collections.singleton(newTopic));

        return createResult.all().toCompletionStage().toCompletableFuture()
                .handle((void1, throwable) -> {
                    if (throwable != null) {
                        // Check if topic already exists (race condition)
                        if (throwable.getCause() != null &&
                                throwable.getCause().getClass().getSimpleName().equals("TopicExistsException")) {
                            log.info("DLQ topic {} already exists (created by another broker)", dlqTopicName);
                            topicCreationCache.put(dlqTopicName, true);
                            return null;
                        }

                        // Creation failed
                        log.error("Failed to create DLQ topic {} for source topic {}",
                                dlqTopicName, sourceTopic, throwable);
                        topicCreationCache.put(dlqTopicName, false);
                        throw new RuntimeException("DLQ topic creation failed for: " + dlqTopicName, throwable);
                    } else {
                        // Creation succeeded
                        log.info("Successfully created DLQ topic {} for source topic {}", dlqTopicName, sourceTopic);
                        topicCreationCache.put(dlqTopicName, true);
                        return null;
                    }
                });
    }

    /**
     * Builds a NewTopic configuration for DLQ topic creation.
     *
     * @param dlqTopicName The DLQ topic name
     * @return NewTopic with configured partitions, replication, and retention settings
     */
    private NewTopic buildDLQTopicConfig(String dlqTopicName) {
        int numPartitions = config.shareGroupDlqNumPartitions();
        short replicationFactor = config.shareGroupDlqReplicationFactor();

        NewTopic newTopic = new NewTopic(dlqTopicName, numPartitions, replicationFactor);

        // Apply topic-level configurations
        Map<String, String> topicConfigs = new HashMap<>();

        // Set retention time
        topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.shareGroupDlqRetentionMs()));

        // Set cleanup policy to delete (DLQ topics should not be compacted)
        topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

        // Set compression type if not "producer" (producer means inherit from source)
        String compressionType = config.shareGroupDlqCompressionType();
        if (!compressionType.equalsIgnoreCase("producer")) {
            topicConfigs.put(TopicConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }

        // Set segment size to 1GB (reasonable for DLQ topics)
        topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(604800000)); // 7 days

        newTopic.configs(topicConfigs);
        return newTopic;
    }

    /**
     * Builds the DLQ topic name from the source topic name.
     *
     * @param sourceTopic The source topic name
     * @return The DLQ topic name ({prefix}{sourceTopic})
     */
    public String buildDLQTopicName(String sourceTopic) {
        return config.shareGroupDlqTopicPrefix() + sourceTopic;
    }

    /**
     * Checks if a DLQ topic has been successfully created (based on cache).
     *
     * @param sourceTopic The source topic name
     * @return true if topic was successfully created, false otherwise
     */
    public boolean isDLQTopicCreated(String sourceTopic) {
        String dlqTopicName = buildDLQTopicName(sourceTopic);
        return Boolean.TRUE.equals(topicCreationCache.get(dlqTopicName));
    }

    /**
     * Invalidates the cache entry for a DLQ topic.
     * Useful when topic is manually deleted and needs to be recreated.
     *
     * @param sourceTopic The source topic name
     */
    public void invalidateCache(String sourceTopic) {
        String dlqTopicName = buildDLQTopicName(sourceTopic);
        topicCreationCache.remove(dlqTopicName);
        log.info("Invalidated DLQ topic cache for {}", dlqTopicName);
    }

    /**
     * Returns the number of cached DLQ topics.
     * Useful for monitoring and testing.
     *
     * @return Number of topics in cache
     */
    public int cacheSize() {
        return topicCreationCache.size();
    }

    /**
     * Clears the entire topic creation cache.
     * Use with caution - mainly for testing purposes.
     */
    public void clearCache() {
        topicCreationCache.clear();
        log.info("Cleared DLQ topic creation cache");
    }

    /**
     * Closes the DLQ topic manager and releases resources.
     */
    public void close() {
        if (adminClient != null) {
            try {
                adminClient.close();
                log.info("Closed DLQ topic manager admin client");
            } catch (Exception e) {
                log.error("Error closing DLQ topic manager admin client", e);
            }
        }
    }
}
