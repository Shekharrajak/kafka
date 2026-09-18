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
import org.apache.kafka.clients.consumer.ShareAcknowledgements;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class ShareIngressForwarder {
    private final Producer<byte[], byte[]> producer;
    private final StreamsProducer streamsProducer;

    ShareIngressForwarder(final Producer<byte[], byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer cannot be null");
        streamsProducer = null;
    }

    ShareIngressForwarder(final StreamsProducer streamsProducer) {
        this.streamsProducer = Objects.requireNonNull(streamsProducer, "streamsProducer cannot be null");
        producer = null;
    }

    void forward(final ShareSource.PollResult batch, final ShareIngressAssignment assignment) {
        Objects.requireNonNull(batch, "batch cannot be null");
        Objects.requireNonNull(assignment, "assignment cannot be null");
        final List<ForwardedRecord> forwardedRecords = prepare(batch, assignment);
        write(forwardedRecords);
        acknowledge(batch, forwardedRecords);
        final ShareAcknowledgements acknowledgements = batch.acknowledgementOwner().acknowledgementsForTransaction();
        if (streamsProducer != null) {
            streamsProducer.sendShareAcknowledgementsToTransaction(acknowledgements, batch.identity().shareGroupMetadata());
        } else {
            producer.sendShareAcknowledgementsToTransaction(acknowledgements, batch.identity().shareGroupMetadata());
        }
    }

    void forwardAtLeastOnce(final ShareSource.PollResult batch, final ShareIngressAssignment assignment) {
        Objects.requireNonNull(batch, "batch cannot be null");
        Objects.requireNonNull(assignment, "assignment cannot be null");
        final List<ForwardedRecord> forwardedRecords = prepare(batch, assignment);
        write(forwardedRecords);
        producer.flush();
        acknowledge(batch, forwardedRecords);
        throwIfAcknowledgementFailed(batch.acknowledgementOwner().completeAcknowledgementsSynchronously());
    }

    private List<ForwardedRecord> prepare(final ShareSource.PollResult batch,
                                          final ShareIngressAssignment assignment) {
        final List<ForwardedRecord> forwardedRecords = new ArrayList<>();
        for (final ConsumerRecord<byte[], byte[]> sourceRecord : batch.records()) {
            final TopicPartition sourcePartition = new TopicPartition(sourceRecord.topic(), sourceRecord.partition());
            final TaskId targetTaskId = assignment.targetTask(sourcePartition);
            final TopicPartition ingressPartition = assignment.ingressPartition(sourcePartition);
            forwardedRecords.add(new ForwardedRecord(sourceRecord, targetTaskId, ingressPartition));
        }
        return forwardedRecords;
    }

    private void write(final List<ForwardedRecord> forwardedRecords) {
        for (final ForwardedRecord forwardedRecord : forwardedRecords) {
            final ProducerRecord<byte[], byte[]> ingressRecord = new ProducerRecord<>(
                forwardedRecord.ingressPartition.topic(),
                forwardedRecord.ingressPartition.partition(),
                null,
                ShareIngressRecordSerde.serialize(new ShareIngressRecord(forwardedRecord.targetTaskId, forwardedRecord.sourceRecord))
            );
            if (streamsProducer != null) {
                streamsProducer.send(ingressRecord, null);
            } else {
                producer.send(ingressRecord);
            }
        }
    }

    private static void acknowledge(final ShareSource.PollResult batch, final List<ForwardedRecord> forwardedRecords) {
        for (final ForwardedRecord forwardedRecord : forwardedRecords) {
            batch.acknowledgementOwner().acknowledge(forwardedRecord.sourceRecord, AcknowledgeType.ACCEPT);
        }
    }

    private static void throwIfAcknowledgementFailed(final Map<TopicIdPartition, Optional<KafkaException>> results) {
        for (final Optional<KafkaException> result : results.values()) {
            if (result.isPresent()) {
                throw result.get();
            }
        }
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
