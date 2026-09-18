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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareAcknowledgements;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.consumer.ShareGroupMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

final class ShareSource implements AutoCloseable {
    enum Type {
        CONSUMER,
        SHARE
    }

    private final ShareConsumer<byte[], byte[]> consumer;
    private AcknowledgementOwner currentAcknowledgementOwner;

    ShareSource(final ShareConsumer<byte[], byte[]> consumer) {
        this.consumer = Objects.requireNonNull(consumer, "consumer cannot be null");
    }

    PollResult poll(final Duration timeout) {
        if (currentAcknowledgementOwner != null && !currentAcknowledgementOwner.allRecordsAreDecided()) {
            throw new IllegalStateException("Cannot poll while the current share batch has undecided records.");
        }
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
        final ShareGroupMetadata metadata = consumer.shareGroupMetadata();
        currentAcknowledgementOwner = new AcknowledgementOwner(consumer, metadata, records);
        return new PollResult(records, new Identity(metadata), currentAcknowledgementOwner);
    }

    @Override
    public void close() {
        consumer.close();
    }

    static final class Identity {
        private final ShareGroupMetadata shareGroupMetadata;

        private Identity(final ShareGroupMetadata shareGroupMetadata) {
            this.shareGroupMetadata = Objects.requireNonNull(shareGroupMetadata, "shareGroupMetadata cannot be null");
        }

        Type type() {
            return Type.SHARE;
        }

        ShareGroupMetadata shareGroupMetadata() {
            return shareGroupMetadata;
        }
    }

    static final class PollResult {
        private final ConsumerRecords<byte[], byte[]> records;
        private final Identity identity;
        private final AcknowledgementOwner acknowledgementOwner;

        private PollResult(final ConsumerRecords<byte[], byte[]> records,
                           final Identity identity,
                           final AcknowledgementOwner acknowledgementOwner) {
            this.records = Objects.requireNonNull(records, "records cannot be null");
            this.identity = Objects.requireNonNull(identity, "identity cannot be null");
            this.acknowledgementOwner = Objects.requireNonNull(acknowledgementOwner, "acknowledgementOwner cannot be null");
        }

        ConsumerRecords<byte[], byte[]> records() {
            return records;
        }

        Identity identity() {
            return identity;
        }

        AcknowledgementOwner acknowledgementOwner() {
            return acknowledgementOwner;
        }
    }

    static final class AcknowledgementOwner {
        private final ShareConsumer<byte[], byte[]> consumer;
        private final ShareGroupMetadata shareGroupMetadata;
        private final Set<RecordIdentity> undecidedRecords;
        private final Set<RecordIdentity> releasedRecords;

        private AcknowledgementOwner(final ShareConsumer<byte[], byte[]> consumer,
                                     final ShareGroupMetadata shareGroupMetadata,
                                     final ConsumerRecords<byte[], byte[]> records) {
            this.consumer = Objects.requireNonNull(consumer, "consumer cannot be null");
            this.shareGroupMetadata = Objects.requireNonNull(shareGroupMetadata, "shareGroupMetadata cannot be null");
            this.undecidedRecords = new HashSet<>();
            this.releasedRecords = new HashSet<>();
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                undecidedRecords.add(new RecordIdentity(record));
            }
        }

        ShareGroupMetadata shareGroupMetadata() {
            return shareGroupMetadata;
        }

        void acknowledge(final ConsumerRecord<byte[], byte[]> record, final AcknowledgeType type) {
            if (type == AcknowledgeType.RENEW) {
                throw new IllegalStateException("ShareSource does not support RENEW acknowledgements.");
            }
            final RecordIdentity identity = new RecordIdentity(record);
            if (!undecidedRecords.contains(identity)) {
                throw new IllegalStateException("The record is not awaiting a share acknowledgement.");
            }
            consumer.acknowledge(record, type);
            undecidedRecords.remove(identity);
            if (type == AcknowledgeType.RELEASE) {
                releasedRecords.add(identity);
            }
        }

        ShareAcknowledgements acknowledgementsForTransaction() {
            if (!allRecordsAreDecided()) {
                throw new IllegalStateException("Cannot extract transaction acknowledgements while the current share batch has undecided records.");
            }
            if (!releasedRecords.isEmpty()) {
                throw new IllegalStateException("Cannot extract transaction acknowledgements containing RELEASE decisions.");
            }
            return consumer.acknowledgementsForTransaction();
        }

        Map<TopicIdPartition, Optional<KafkaException>> completeAcknowledgementsSynchronously() {
            if (!allRecordsAreDecided()) {
                throw new IllegalStateException("Cannot commit share acknowledgements while the current share batch has undecided records.");
            }
            return consumer.commitSync();
        }

        private boolean allRecordsAreDecided() {
            return undecidedRecords.isEmpty();
        }
    }

    private static final class RecordIdentity {
        private final String topic;
        private final int partition;
        private final long offset;

        private RecordIdentity(final ConsumerRecord<byte[], byte[]> record) {
            this.topic = record.topic();
            this.partition = record.partition();
            this.offset = record.offset();
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof RecordIdentity)) {
                return false;
            }
            final RecordIdentity that = (RecordIdentity) other;
            return partition == that.partition && offset == that.offset && topic.equals(that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, offset);
        }
    }
}
