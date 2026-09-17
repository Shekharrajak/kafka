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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShareIngressForwarderTest {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    @SuppressWarnings("unchecked")
    @Test
    void shouldWriteIngressBeforeStagingShareAcknowledgements() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final ShareGroupMetadata metadata = new ShareGroupMetadata("share-group", "member", 3);
        final ShareAcknowledgements acknowledgements = ShareAcknowledgements.empty();
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("source", 1, 2L, new byte[] {1}, new byte[] {2});
        final ShareSource.PollResult batch = poll(consumer, metadata, acknowledgements, record);
        final ShareIngressAssignment assignment = new ShareIngressAssignment(
            "application",
            Map.of(new TaskId(0, 1), Set.of(new TopicPartition("source", 1)))
        );

        new ShareIngressForwarder(producer).forward(batch, assignment);

        final InOrder ordered = inOrder(producer, consumer);
        ordered.verify(producer).send(argThat(produced ->
            produced.topic().equals("application-source-share-ingress") && produced.partition() == 1));
        ordered.verify(consumer).acknowledge(record, AcknowledgeType.ACCEPT);
        ordered.verify(consumer).acknowledgementsForTransaction();
        ordered.verify(producer).sendShareAcknowledgementsToTransaction(same(acknowledgements), same(metadata));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldNotStageShareAcknowledgementsWhenIngressWriteFails() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final ShareGroupMetadata metadata = new ShareGroupMetadata("share-group", "member", 3);
        final ShareAcknowledgements acknowledgements = ShareAcknowledgements.empty();
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("source", 0, 2L, new byte[] {1}, new byte[] {2});
        final ShareSource.PollResult batch = poll(consumer, metadata, acknowledgements, record);
        final ShareIngressAssignment assignment = new ShareIngressAssignment(
            "application",
            Map.of(new TaskId(0, 0), Set.of(new TopicPartition("source", 0)))
        );
        when(producer.send(any())).thenThrow(new KafkaException("write failed"));

        assertThrows(KafkaException.class, () -> new ShareIngressForwarder(producer).forward(batch, assignment));

        verify(consumer, never()).acknowledge(record, AcknowledgeType.ACCEPT);
        verify(producer, never()).sendShareAcknowledgementsToTransaction(any(), any());
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldValidateTheWholeBatchBeforeWritingIngress() {
        final Producer<byte[], byte[]> producer = mock(Producer.class);
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final ShareGroupMetadata metadata = new ShareGroupMetadata("share-group", "member", 3);
        final ConsumerRecord<byte[], byte[]> firstRecord = new ConsumerRecord<>("source", 0, 2L, new byte[] {1}, new byte[] {2});
        final ConsumerRecord<byte[], byte[]> unsupportedRecord = new ConsumerRecord<>("source", 1, 3L, new byte[] {3}, new byte[] {4});
        when(consumer.poll(POLL_TIMEOUT)).thenReturn(new ConsumerRecords<>(
            Map.of(
                new TopicPartition("source", 0), List.of(firstRecord),
                new TopicPartition("source", 1), List.of(unsupportedRecord)
            ),
            Map.of()
        ));
        when(consumer.shareGroupMetadata()).thenReturn(metadata);
        final ShareSource.PollResult batch = new ShareSource(consumer).poll(POLL_TIMEOUT);
        final ShareIngressAssignment assignment = new ShareIngressAssignment(
            "application",
            Map.of(new TaskId(0, 0), Set.of(new TopicPartition("source", 0)))
        );

        assertThrows(IllegalArgumentException.class, () -> new ShareIngressForwarder(producer).forward(batch, assignment));

        verify(producer, never()).send(any());
        verify(consumer, never()).acknowledge(any(), any());
    }

    private static ShareSource.PollResult poll(final ShareConsumer<byte[], byte[]> consumer,
                                               final ShareGroupMetadata metadata,
                                               final ShareAcknowledgements acknowledgements,
                                               final ConsumerRecord<byte[], byte[]> record) {
        when(consumer.poll(POLL_TIMEOUT)).thenReturn(new ConsumerRecords<>(
            Map.of(new TopicPartition(record.topic(), record.partition()), List.of(record)),
            Map.of()
        ));
        when(consumer.shareGroupMetadata()).thenReturn(metadata);
        when(consumer.acknowledgementsForTransaction()).thenReturn(acknowledgements);
        return new ShareSource(consumer).poll(POLL_TIMEOUT);
    }
}
