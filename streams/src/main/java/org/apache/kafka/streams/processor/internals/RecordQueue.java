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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.processor.internals.metrics.TopicMetrics;

import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.streams.processor.internals.ClientUtils.consumerRecordSizeInBytes;

/**
 * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumerRecord + timestamp). It also keeps track of the
 * partition timestamp defined as the largest timestamp seen on the partition so far; this is passed to the
 * timestamp extractor.
 */
public class RecordQueue {

    public static final long UNKNOWN = ConsumerRecord.NO_TIMESTAMP;

    private final Logger log;
    private final SourceNode<?, ?> source;
    private final TopicPartition partition;
    private final ProcessorContext<?, ?> processorContext;
    private final TimestampExtractor timestampExtractor;
    private final RecordDeserializer recordDeserializer;
    private final ArrayDeque<QueuedRecord> fifoQueue;

    private StampedRecord headRecord = null;
    private long partitionTime = UNKNOWN;

    private final Sensor droppedRecordsSensor;
    private final Sensor consumedSensor;
    private long headRecordSizeInBytes;

    RecordQueue(final TopicPartition partition,
                final SourceNode<?, ?> source,
                final TimestampExtractor timestampExtractor,
                final DeserializationExceptionHandler deserializationExceptionHandler,
                final InternalProcessorContext<?, ?> processorContext,
                final LogContext logContext) {
        this.source = source;
        this.partition = partition;
        this.fifoQueue = new ArrayDeque<>();
        this.timestampExtractor = timestampExtractor;
        this.processorContext = processorContext;

        final String threadName = Thread.currentThread().getName();
        droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
            threadName,
            processorContext.taskId().toString(),
            processorContext.metrics()
        );
        consumedSensor = TopicMetrics.consumedSensor(
            threadName,
            processorContext.taskId().toString(),
            source.name(),
            partition.topic(),
            processorContext.metrics()
        );
        recordDeserializer = new RecordDeserializer(
            source,
            deserializationExceptionHandler,
            logContext,
            droppedRecordsSensor
        );
        this.log = logContext.logger(RecordQueue.class);
        this.headRecordSizeInBytes = 0L;
    }

    void setPartitionTime(final long partitionTime) {
        this.partitionTime = partitionTime;
    }

    /**
     * Returns the corresponding source node in the topology
     *
     * @return SourceNode
     */
    public SourceNode<?, ?> source() {
        return source;
    }

    /**
     * Returns the partition with which this queue is associated
     *
     * @return TopicPartition
     */
    public TopicPartition partition() {
        return partition;
    }

    /**
     * Add a batch of {@link ConsumerRecord} into the queue
     *
     * @param rawRecords the raw records
     * @return the size of this queue
     */
    int addRawRecords(final Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        for (final ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
            fifoQueue.addLast(new QueuedRecord(rawRecord, rawRecord.offset(), rawRecord.leaderEpoch()));
        }

        updateHead();

        return size();
    }

    int addShareIngressRecords(final TaskId taskId,
                               final Iterable<ConsumerRecord<byte[], byte[]>> ingressRecords,
                               final ShareIngressAssignment assignment) {
        final List<QueuedRecord> recordsToAdd = new ArrayList<>();
        for (final ConsumerRecord<byte[], byte[]> ingressRecord : ingressRecords) {
            final ShareIngressDelivery delivery = ShareIngressDelivery.decode(taskId, ingressRecord, assignment);
            if (!partition.equals(delivery.ingressPartition())) {
                throw new IllegalArgumentException("Ingress record partition " + delivery.ingressPartition()
                    + " does not match queue partition " + partition);
            }
            recordsToAdd.add(new QueuedRecord(
                delivery.sourceRecord().sourceConsumerRecord(),
                delivery.ingressOffset(),
                delivery.ingressLeaderEpoch()
            ));
        }

        fifoQueue.addAll(recordsToAdd);
        updateHead();

        return size();
    }

    /**
     * Get the next {@link StampedRecord} from the queue
     *
     * @return StampedRecord
     */
    public StampedRecord poll(final long wallClockTime) {
        final StampedRecord recordToReturn = headRecord;

        consumedSensor.record(headRecordSizeInBytes, wallClockTime);

        headRecord = null;
        headRecordSizeInBytes = 0L;
        partitionTime = Math.max(partitionTime, recordToReturn.timestamp);

        updateHead();

        return recordToReturn;
    }

    /**
     * Returns the number of records in the queue
     *
     * @return the number of records
     */
    public int size() {
        // plus one deserialized head record for timestamp tracking
        return fifoQueue.size() + (headRecord == null ? 0 : 1);
    }

    /**
     * Tests if the queue is empty
     *
     * @return true if the queue is empty, otherwise false
     */
    public boolean isEmpty() {
        return fifoQueue.isEmpty() && headRecord == null;
    }

    /**
     * Returns the head record's timestamp
     *
     * @return timestamp
     */
    public long headRecordTimestamp() {
        return headRecord == null ? UNKNOWN : headRecord.timestamp;
    }

    public Long headRecordOffset() {
        return headRecord == null ? null : headRecord.inputOffset();
    }

    /**
     * Returns the leader epoch of the head record if it exists
     *
     * @return An Optional containing the leader epoch of the head record, or null if the queue is empty. The Optional.empty()
     * is reserved for the case  when the leader epoch is not set for head record of the queue.
     */
    @SuppressWarnings("OptionalAssignedToNull")
    public Optional<Integer> headRecordLeaderEpoch() {
        return headRecord == null ? null : headRecord.inputLeaderEpoch();
    }

    /**
     * Clear the fifo queue of its elements
     */
    public void clear() {
        fifoQueue.clear();
        headRecord = null;
        headRecordSizeInBytes = 0L;
        partitionTime = UNKNOWN;
    }

    public void close() {
        processorContext.metrics().removeSensor(consumedSensor);
    }

    private void updateHead() {
        QueuedRecord lastCorruptedRecord = null;

        while (headRecord == null && !fifoQueue.isEmpty()) {
            final QueuedRecord queuedRecord = fifoQueue.pollFirst();
            final ConsumerRecord<byte[], byte[]> raw = queuedRecord.record;
            final ConsumerRecord<Object, Object> deserialized =
                recordDeserializer.deserialize(processorContext, raw);

            if (deserialized == null) {
                // this only happens if the deserializer decides to skip. It has already logged the reason.
                lastCorruptedRecord = queuedRecord;
                continue;
            }

            final long timestamp;
            try {
                timestamp = timestampExtractor.extract(deserialized, partitionTime);
            } catch (final StreamsException internalFatalExtractorException) {
                throw internalFatalExtractorException;
            } catch (final Exception fatalUserException) {
                throw new StreamsException(
                        String.format("Fatal user code error in TimestampExtractor callback for record %s.", deserialized),
                        fatalUserException);
            }
            log.trace("Source node {} extracted timestamp {} for record {}", source.name(), timestamp, deserialized);

            // drop message if TS is invalid, i.e., negative
            if (timestamp < 0) {
                log.warn(
                        "Skipping record due to negative extracted timestamp. topic=[{}] partition=[{}] offset=[{}] extractedTimestamp=[{}] extractor=[{}]",
                        deserialized.topic(), deserialized.partition(), deserialized.offset(), timestamp, timestampExtractor.getClass().getCanonicalName()
                );
                droppedRecordsSensor.record();
                lastCorruptedRecord = queuedRecord;
                continue;
            }
            headRecord = new StampedRecord(
                deserialized,
                timestamp,
                raw.key(),
                raw.value(),
                queuedRecord.inputOffset,
                queuedRecord.inputLeaderEpoch
            );
            headRecordSizeInBytes = consumerRecordSizeInBytes(raw);
        }

        // if all records in the FIFO queue are corrupted, make the last one the headRecord
        // This record is used to update the offsets. See KAFKA-6502 for more details.
        if (headRecord == null && lastCorruptedRecord != null) {
            headRecord = new CorruptedRecord(
                lastCorruptedRecord.record,
                lastCorruptedRecord.inputOffset,
                lastCorruptedRecord.inputLeaderEpoch
            );
        }
    }

    private static final class QueuedRecord {
        private final ConsumerRecord<byte[], byte[]> record;
        private final long inputOffset;
        private final Optional<Integer> inputLeaderEpoch;

        private QueuedRecord(final ConsumerRecord<byte[], byte[]> record,
                             final long inputOffset,
                             final Optional<Integer> inputLeaderEpoch) {
            this.record = record;
            this.inputOffset = inputOffset;
            this.inputLeaderEpoch = inputLeaderEpoch;
        }
    }

    /**
     * @return the local partitionTime for this particular RecordQueue
     */
    long partitionTime() {
        return partitionTime;
    }
}
