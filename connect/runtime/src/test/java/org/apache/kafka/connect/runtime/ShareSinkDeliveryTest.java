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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ShareSinkDeliveryTest {
    private final ShareSinkTask task = mock(ShareSinkTask.class);
    @SuppressWarnings("unchecked")
    private final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
    private final TopicPartition partition = new TopicPartition("events", 0);
    private final Duration timeout = Duration.ofSeconds(10);

    private ConsumerRecords<byte[], byte[]> records() {
        return new ConsumerRecords<>(Map.of(partition, List.of(
                new ConsumerRecord<>("events", 0, 101, null, null),
                new ConsumerRecord<>("events", 0, 100, null, null))));
    }

    private SinkRecord convert(ConsumerRecord<byte[], byte[]> record) {
        return new SinkRecord("transformed", 9, null, null, null, "value", record.offset());
    }

    @Test
    void acknowledgesOriginalRecordsOnlyAfterDurableWrite() {
        when(consumer.commitSync(timeout)).thenReturn(Map.of());
        ConsumerRecords<byte[], byte[]> records = records();
        new ShareSinkDelivery(records, this::convert).complete(task, consumer, timeout);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SinkRecord>> captured = ArgumentCaptor.forClass(Collection.class);
        InOrder order = inOrder(task, consumer);
        order.verify(task).put(captured.capture());
        for (ConsumerRecord<byte[], byte[]> record : records) {
            order.verify(consumer).acknowledge(record, AcknowledgeType.ACCEPT);
        }
        order.verify(consumer).commitSync(timeout);
        assertEquals(List.of(101L, 100L), captured.getValue().stream().map(SinkRecord::kafkaOffset).toList());
    }

    @Test
    void sinkFailureDoesNotAcknowledge() {
        doThrow(new RetriableException("sink unavailable")).when(task).put(anyCollection());
        ShareSinkDelivery delivery = new ShareSinkDelivery(records(), this::convert);
        assertThrows(RetriableException.class, () -> delivery.complete(task, consumer, timeout));
        verifyNoInteractions(consumer);
    }

    @Test
    void filteringCompletesOriginalDeliveriesWithoutSinkWrites() {
        when(consumer.commitSync(timeout)).thenReturn(Map.of());
        ConsumerRecords<byte[], byte[]> records = records();
        new ShareSinkDelivery(records, record -> null).complete(task, consumer, timeout);
        verify(task, never()).put(anyCollection());
        for (ConsumerRecord<byte[], byte[]> record : records) {
            verify(consumer).acknowledge(record, AcknowledgeType.ACCEPT);
        }
    }

    @Test
    void conversionFailurePreventsAnyWriteOrAcknowledgement() {
        assertThrows(ConnectException.class, () -> new ShareSinkDelivery(records(), record -> {
            if (record.offset() == 100) {
                throw new ConnectException("conversion failed");
            }
            return convert(record);
        }));
        verifyNoInteractions(task, consumer);
    }

    @Test
    void checksPartitionAcknowledgementFailures() {
        when(consumer.commitSync(timeout)).thenReturn(Map.of(
                new TopicIdPartition(Uuid.randomUuid(), partition), Optional.of(new KafkaException("lock expired"))));
        assertThrows(KafkaException.class, () -> new ShareSinkDelivery(records(), this::convert).complete(task, consumer, timeout));
        verify(task).put(anyCollection());
    }

    @Test
    void checksRequestAcknowledgementFailures() {
        when(consumer.commitSync(timeout)).thenThrow(new KafkaException("broker unavailable"));
        assertThrows(KafkaException.class, () -> new ShareSinkDelivery(records(), this::convert).complete(task, consumer, timeout));
    }

    @Test
    void emptyBatchHasNoSideEffects() {
        new ShareSinkDelivery(ConsumerRecords.empty(), this::convert).complete(task, consumer, timeout);
        verifyNoInteractions(task, consumer);
    }

    @Test
    void acknowledgementStagingFailureDoesNotCommitPartialBatch() {
        doThrow(new KafkaException("invalid record state")).when(consumer).acknowledge(any(), any());
        assertThrows(KafkaException.class, () -> new ShareSinkDelivery(records(), this::convert).complete(task, consumer, timeout));
        verify(consumer, never()).commitSync(any(Duration.class));
    }
}
