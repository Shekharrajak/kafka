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

import java.time.Duration;
import java.util.Objects;

final class ShareSource implements AutoCloseable {
    enum Type {
        CONSUMER,
        SHARE
    }

    private final ShareConsumer<byte[], byte[]> consumer;

    ShareSource(final ShareConsumer<byte[], byte[]> consumer) {
        this.consumer = Objects.requireNonNull(consumer, "consumer cannot be null");
    }

    PollResult poll(final Duration timeout) {
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
        final ShareGroupMetadata metadata = consumer.shareGroupMetadata();
        return new PollResult(records, new Identity(metadata), new AcknowledgementOwner(consumer, metadata));
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

        private AcknowledgementOwner(final ShareConsumer<byte[], byte[]> consumer,
                                     final ShareGroupMetadata shareGroupMetadata) {
            this.consumer = Objects.requireNonNull(consumer, "consumer cannot be null");
            this.shareGroupMetadata = Objects.requireNonNull(shareGroupMetadata, "shareGroupMetadata cannot be null");
        }

        ShareGroupMetadata shareGroupMetadata() {
            return shareGroupMetadata;
        }

        void acknowledge(final ConsumerRecord<byte[], byte[]> record, final AcknowledgeType type) {
            consumer.acknowledge(record, type);
        }

        ShareAcknowledgements acknowledgementsForTransaction() {
            return consumer.acknowledgementsForTransaction();
        }
    }
}
