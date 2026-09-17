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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class ShareIngressForwarder {
    private final Producer<byte[], byte[]> producer;

    ShareIngressForwarder(final Producer<byte[], byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer cannot be null");
    }

    void forward(final ShareSource.PollResult batch, final ShareIngressAssignment assignment) {
        Objects.requireNonNull(batch, "batch cannot be null");
        Objects.requireNonNull(assignment, "assignment cannot be null");
        final List<ForwardedRecord> forwardedRecords = new ArrayList<>();
        for (final ConsumerRecord<byte[], byte[]> sourceRecord : batch.records()) {
            final TopicPartition sourcePartition = new TopicPartition(sourceRecord.topic(), sourceRecord.partition());
            final TaskId targetTaskId = assignment.targetTask(sourcePartition);
            final TopicPartition ingressPartition = assignment.ingressPartition(sourcePartition);
            forwardedRecords.add(new ForwardedRecord(sourceRecord, targetTaskId, ingressPartition));
        }
        for (final ForwardedRecord forwardedRecord : forwardedRecords) {
            producer.send(new ProducerRecord<>(
                forwardedRecord.ingressPartition.topic(),
                forwardedRecord.ingressPartition.partition(),
                null,
                ShareIngressRecordSerde.serialize(new ShareIngressRecord(forwardedRecord.targetTaskId, forwardedRecord.sourceRecord))
            ));
        }
        for (final ForwardedRecord forwardedRecord : forwardedRecords) {
            batch.acknowledgementOwner().acknowledge(forwardedRecord.sourceRecord, AcknowledgeType.ACCEPT);
        }
        producer.sendShareAcknowledgementsToTransaction(
            batch.acknowledgementOwner().acknowledgementsForTransaction(),
            batch.identity().shareGroupMetadata()
        );
    }

    private static final class ForwardedRecord {
        private final ConsumerRecord<byte[], byte[]> sourceRecord;
        private final TaskId targetTaskId;
        private final TopicPartition ingressPartition;

        private ForwardedRecord(final ConsumerRecord<byte[], byte[]> sourceRecord,
                                final TaskId targetTaskId,
                                final TopicPartition ingressPartition) {
            this.sourceRecord = sourceRecord;
            this.targetTaskId = targetTaskId;
            this.ingressPartition = ingressPartition;
        }
    }
}
