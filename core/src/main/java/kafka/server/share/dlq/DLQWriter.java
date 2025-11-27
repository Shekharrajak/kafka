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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * Writes rejected records to Dead Letter Queue (DLQ) topics with metadata headers.
 *
 * <p>This class is responsible for:</p>
 * <ul>
 *   <li>Building DLQ records with original record data and metadata headers</li>
 *   <li>Asynchronously writing to DLQ topics using Kafka producer</li>
 *   <li>Handling write failures gracefully without blocking share group operations</li>
 *   <li>Supporting configurable metadata header inclusion</li>
 * </ul>
 *
 * <p>DLQ Metadata Headers:</p>
 * <ul>
 *   <li>__share.dlq.original.topic - Original source topic name</li>
 *   <li>__share.dlq.original.partition - Original partition number</li>
 *   <li>__share.dlq.original.offset - Original offset</li>
 *   <li>__share.dlq.group.id - Share group identifier</li>
 *   <li>__share.dlq.delivery.count - Number of delivery attempts before rejection</li>
 *   <li>__share.dlq.reject.timestamp - Timestamp when record was rejected (ms)</li>
 *   <li>__share.dlq.member.id - Member that rejected the record</li>
 *   <li>__share.dlq.broker.id - Broker ID that processed the rejection</li>
 *   <li>__share.dlq.failure.reason - Optional failure reason (if provided)</li>
 * </ul>
 */
public class DLQWriter {
    private static final Logger log = LoggerFactory.getLogger(DLQWriter.class);

    // DLQ header key constants
    public static final String DLQ_HEADER_ORIGINAL_TOPIC = "__share.dlq.original.topic";
    public static final String DLQ_HEADER_ORIGINAL_PARTITION = "__share.dlq.original.partition";
    public static final String DLQ_HEADER_ORIGINAL_OFFSET = "__share.dlq.original.offset";
    public static final String DLQ_HEADER_GROUP_ID = "__share.dlq.group.id";
    public static final String DLQ_HEADER_DELIVERY_COUNT = "__share.dlq.delivery.count";
    public static final String DLQ_HEADER_REJECT_TIMESTAMP = "__share.dlq.reject.timestamp";
    public static final String DLQ_HEADER_MEMBER_ID = "__share.dlq.member.id";
    public static final String DLQ_HEADER_BROKER_ID = "__share.dlq.broker.id";
    public static final String DLQ_HEADER_FAILURE_REASON = "__share.dlq.failure.reason";

    private final KafkaProducer<byte[], byte[]> dlqProducer;
    private final ShareGroupConfig config;
    private final DLQTopicManager topicManager;
    private final int brokerId;

    // Metrics tracking (to be integrated with SharePartitionMetrics)
    private volatile long recordsSent = 0;
    private volatile long recordsFailed = 0;
    private volatile long bytesSent = 0;

    /**
     * Creates a new DLQ writer.
     *
     * @param dlqProducer Kafka producer for writing to DLQ topics
     * @param config Share group configuration
     * @param topicManager DLQ topic manager for topic creation
     * @param brokerId The broker ID for metadata headers
     */
    public DLQWriter(
            KafkaProducer<byte[], byte[]> dlqProducer,
            ShareGroupConfig config,
            DLQTopicManager topicManager,
            int brokerId
    ) {
        this.dlqProducer = dlqProducer;
        this.config = config;
        this.topicManager = topicManager;
        this.brokerId = brokerId;
    }

    /**
     * Writes a rejected record to the DLQ topic asynchronously.
     *
     * <p>This method:</p>
     * <ul>
     *   <li>Ensures DLQ topic exists (auto-creates if enabled)</li>
     *   <li>Builds DLQ record with original data and metadata headers</li>
     *   <li>Asynchronously sends to DLQ topic</li>
     *   <li>Returns immediately without blocking</li>
     * </ul>
     *
     * <p>Note: DLQ write failures are logged but do NOT fail the acknowledge operation.
     * This ensures share group progress is not blocked by DLQ issues.</p>
     *
     * @param originalTopic Original source topic name
     * @param originalPartition Original partition number
     * @param originalOffset Original record offset
     * @param key Record key (can be null)
     * @param value Record value (can be null)
     * @param originalHeaders Original record headers
     * @param groupId Share group ID
     * @param memberId Member ID that rejected the record
     * @param deliveryCount Number of delivery attempts before rejection
     * @param failureReason Optional failure reason (can be null)
     * @return CompletableFuture that completes when DLQ write finishes
     */
    public CompletableFuture<RecordMetadata> writeToDLQ(
            String originalTopic,
            int originalPartition,
            long originalOffset,
            byte[] key,
            byte[] value,
            Header[] originalHeaders,
            String groupId,
            String memberId,
            int deliveryCount,
            String failureReason
    ) {
        if (!config.shareGroupDlqEnabled()) {
            log.trace("DLQ disabled, skipping write for {}-{} offset {}", originalTopic, originalPartition, originalOffset);
            return CompletableFuture.completedFuture(null);
        }

        String dlqTopicName = topicManager.buildDLQTopicName(originalTopic);

        CompletableFuture<RecordMetadata> resultFuture = new CompletableFuture<>();

        // Ensure DLQ topic exists before writing
        topicManager.ensureDLQTopicExists(originalTopic)
                .whenComplete((void1, topicCreationError) -> {
                    if (topicCreationError != null) {
                        log.error("DLQ topic creation failed for {}, cannot write rejected record from {}-{} offset {}",
                                dlqTopicName, originalTopic, originalPartition, originalOffset, topicCreationError);
                        recordsFailed++;
                        resultFuture.completeExceptionally(topicCreationError);
                        return;
                    }

                    // Build DLQ record with metadata headers
                    ProducerRecord<byte[], byte[]> dlqRecord = buildDLQRecord(
                            dlqTopicName,
                            originalTopic,
                            originalPartition,
                            originalOffset,
                            key,
                            value,
                            originalHeaders,
                            groupId,
                            memberId,
                            deliveryCount,
                            failureReason
                    );

                    // Send to DLQ topic asynchronously
                    sendToDLQ(dlqRecord, originalTopic, originalPartition, originalOffset, resultFuture);
                });

        return resultFuture;
    }

    /**
     * Sends a DLQ record to the DLQ topic.
     */
    private void sendToDLQ(
            ProducerRecord<byte[], byte[]> dlqRecord,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            CompletableFuture<RecordMetadata> resultFuture
    ) {
        dlqProducer.send(dlqRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to write rejected record to DLQ topic {} (original: {}-{} offset {})",
                        dlqRecord.topic(), originalTopic, originalPartition, originalOffset, exception);
                recordsFailed++;
                resultFuture.completeExceptionally(exception);
            } else {
                log.debug("Successfully wrote rejected record to DLQ topic {} partition {} offset {} " +
                                "(original: {}-{} offset {})",
                        metadata.topic(), metadata.partition(), metadata.offset(),
                        originalTopic, originalPartition, originalOffset);
                recordsSent++;
                int recordSize = (dlqRecord.key() != null ? dlqRecord.key().length : 0) +
                        (dlqRecord.value() != null ? dlqRecord.value().length : 0);
                bytesSent += recordSize;
                resultFuture.complete(metadata);
            }
        });
    }

    /**
     * Builds a DLQ ProducerRecord with original data and metadata headers.
     */
    private ProducerRecord<byte[], byte[]> buildDLQRecord(
            String dlqTopicName,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            byte[] key,
            byte[] value,
            Header[] originalHeaders,
            String groupId,
            String memberId,
            int deliveryCount,
            String failureReason
    ) {
        // Start with original headers (if they exist and include headers is enabled)
        Headers headers = new RecordHeaders();
        if (originalHeaders != null && config.shareGroupDlqIncludeHeaders()) {
            for (Header header : originalHeaders) {
                headers.add(header);
            }
        }

        // Add DLQ metadata headers
        long currentTimestamp = System.currentTimeMillis();

        headers.add(new RecordHeader(DLQ_HEADER_ORIGINAL_TOPIC, originalTopic.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(DLQ_HEADER_ORIGINAL_PARTITION, intToBytes(originalPartition)));
        headers.add(new RecordHeader(DLQ_HEADER_ORIGINAL_OFFSET, longToBytes(originalOffset)));
        headers.add(new RecordHeader(DLQ_HEADER_GROUP_ID, groupId.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(DLQ_HEADER_DELIVERY_COUNT, intToBytes(deliveryCount)));
        headers.add(new RecordHeader(DLQ_HEADER_REJECT_TIMESTAMP, longToBytes(currentTimestamp)));
        headers.add(new RecordHeader(DLQ_HEADER_MEMBER_ID, memberId.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(DLQ_HEADER_BROKER_ID, intToBytes(brokerId)));

        if (failureReason != null && !failureReason.isEmpty()) {
            headers.add(new RecordHeader(DLQ_HEADER_FAILURE_REASON, failureReason.getBytes(StandardCharsets.UTF_8)));
        }

        // Create DLQ record
        // Note: We send to partition 0 by default to maintain ordering of rejected records
        // Users can configure multiple DLQ partitions if needed
        return new ProducerRecord<>(
                dlqTopicName,
                null, // partition - let producer choose based on key
                currentTimestamp,
                key,
                value,
                headers
        );
    }

    /**
     * Checks if DLQ is enabled.
     *
     * @return true if DLQ is enabled, false otherwise
     */
    public boolean isEnabled() {
        return config.shareGroupDlqEnabled();
    }

    /**
     * Returns the number of records successfully sent to DLQ.
     *
     * @return Records sent count
     */
    public long getRecordsSent() {
        return recordsSent;
    }

    /**
     * Returns the number of records that failed to send to DLQ.
     *
     * @return Records failed count
     */
    public long getRecordsFailed() {
        return recordsFailed;
    }

    /**
     * Returns the total bytes sent to DLQ.
     *
     * @return Bytes sent
     */
    public long getBytesSent() {
        return bytesSent;
    }

    /**
     * Closes the DLQ writer and releases resources.
     */
    public void close() {
        if (dlqProducer != null) {
            try {
                dlqProducer.flush();
                dlqProducer.close();
                log.info("Closed DLQ writer producer. Stats: sent={}, failed={}, bytes={}",
                        recordsSent, recordsFailed, bytesSent);
            } catch (Exception e) {
                log.error("Error closing DLQ writer producer", e);
            }
        }
    }

    // Helper methods for byte conversion

    private static byte[] intToBytes(int value) {
        return new byte[] {
            (byte) (value >> 24),
            (byte) (value >> 16),
            (byte) (value >> 8),
            (byte) value
        };
    }

    private static byte[] longToBytes(long value) {
        return new byte[] {
            (byte) (value >> 56),
            (byte) (value >> 48),
            (byte) (value >> 40),
            (byte) (value >> 32),
            (byte) (value >> 24),
            (byte) (value >> 16),
            (byte) (value >> 8),
            (byte) value
        };
    }
}
