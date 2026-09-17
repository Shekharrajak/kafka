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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareAcknowledgements;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.consumer.ShareGroupMetadata;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShareSourceTest {
    @SuppressWarnings("unchecked")
    @Test
    void shouldExposeShareIdentityAndAcknowledgementOwnerFromPoll() {
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final ShareGroupMetadata metadata = new ShareGroupMetadata("share-group", "member", 3);
        final ShareAcknowledgements acknowledgements = ShareAcknowledgements.empty();
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("input", 0, 1L, new byte[0], new byte[0]);
        final ConsumerRecords<byte[], byte[]> records = records(record);
        final Duration timeout = Duration.ofMillis(100);

        when(consumer.poll(timeout)).thenReturn(records);
        when(consumer.shareGroupMetadata()).thenReturn(metadata);
        when(consumer.acknowledgementsForTransaction()).thenReturn(acknowledgements);

        final ShareSource.PollResult result = new ShareSource(consumer).poll(timeout);

        assertThat(result.records(), sameInstance(records));
        assertThat(result.identity().type(), is(ShareSource.Type.SHARE));
        assertThat(result.identity().type(), is(not(ShareSource.Type.CONSUMER)));
        assertThat(result.identity().shareGroupMetadata(), sameInstance(metadata));
        assertThat(result.acknowledgementOwner().shareGroupMetadata(), sameInstance(metadata));

        result.acknowledgementOwner().acknowledge(record, AcknowledgeType.ACCEPT);
        assertThat(result.acknowledgementOwner().acknowledgementsForTransaction(), sameInstance(acknowledgements));

        verify(consumer).acknowledge(record, AcknowledgeType.ACCEPT);
        verify(consumer).acknowledgementsForTransaction();
    }

    @Test
    void shouldNotExposeNormalConsumerOffsetCommitOperations() {
        assertFalse(Consumer.class.isAssignableFrom(ShareSource.class));
        assertFalse(Arrays.stream(ShareSource.class.getDeclaredMethods())
            .anyMatch(method -> method.getName().startsWith("commit")));
        assertFalse(Arrays.stream(ShareSource.AcknowledgementOwner.class.getDeclaredMethods())
            .anyMatch(method -> method.getName().startsWith("commit")));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldRejectPollWhileTheCurrentBatchHasUndecidedRecords() {
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final Duration timeout = Duration.ofMillis(100);
        final ConsumerRecord<byte[], byte[]> record = record(1L);

        when(consumer.poll(timeout)).thenReturn(records(record));
        when(consumer.shareGroupMetadata()).thenReturn(new ShareGroupMetadata("share-group", "member", 3));

        final ShareSource source = new ShareSource(consumer);
        source.poll(timeout);

        final IllegalStateException exception = assertThrows(IllegalStateException.class, () -> source.poll(timeout));

        assertThat(exception.getMessage(), is("Cannot poll while the current share batch has undecided records."));
        verify(consumer).poll(timeout);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldAllowPollOnlyAfterEveryRecordInTheCurrentBatchIsDecided() {
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final Duration timeout = Duration.ofMillis(100);
        final ConsumerRecord<byte[], byte[]> firstRecord = record(1L);
        final ConsumerRecord<byte[], byte[]> secondRecord = record(2L);

        when(consumer.poll(timeout)).thenReturn(records(firstRecord, secondRecord), ConsumerRecords.empty());
        when(consumer.shareGroupMetadata()).thenReturn(new ShareGroupMetadata("share-group", "member", 3));

        final ShareSource source = new ShareSource(consumer);
        final ShareSource.PollResult firstBatch = source.poll(timeout);

        firstBatch.acknowledgementOwner().acknowledge(firstRecord, AcknowledgeType.ACCEPT);
        assertThrows(IllegalStateException.class, () -> source.poll(timeout));

        firstBatch.acknowledgementOwner().acknowledge(secondRecord, AcknowledgeType.REJECT);
        source.poll(timeout);

        verify(consumer, times(2)).poll(timeout);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldRejectRenewAcknowledgementsAtTheShareSourceBoundary() {
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final Duration timeout = Duration.ofMillis(100);
        final ConsumerRecord<byte[], byte[]> record = record(1L);

        when(consumer.poll(timeout)).thenReturn(records(record));
        when(consumer.shareGroupMetadata()).thenReturn(new ShareGroupMetadata("share-group", "member", 3));

        final ShareSource.PollResult batch = new ShareSource(consumer).poll(timeout);

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> batch.acknowledgementOwner().acknowledge(record, AcknowledgeType.RENEW)
        );

        assertThat(exception.getMessage(), is("ShareSource does not support RENEW acknowledgements."));
        verify(consumer, never()).acknowledge(record, AcknowledgeType.RENEW);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldExtractTransactionAcknowledgementsOnlyForFullyDecidedBatch() {
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final Duration timeout = Duration.ofMillis(100);
        final ConsumerRecord<byte[], byte[]> firstRecord = record(1L);
        final ConsumerRecord<byte[], byte[]> secondRecord = record(2L);
        final ShareAcknowledgements acknowledgements = ShareAcknowledgements.empty();

        when(consumer.poll(timeout)).thenReturn(records(firstRecord, secondRecord));
        when(consumer.shareGroupMetadata()).thenReturn(new ShareGroupMetadata("share-group", "member", 3));
        when(consumer.acknowledgementsForTransaction()).thenReturn(acknowledgements);

        final ShareSource.PollResult batch = new ShareSource(consumer).poll(timeout);
        batch.acknowledgementOwner().acknowledge(firstRecord, AcknowledgeType.ACCEPT);

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> batch.acknowledgementOwner().acknowledgementsForTransaction()
        );

        assertThat(exception.getMessage(), is("Cannot extract transaction acknowledgements while the current share batch has undecided records."));
        verify(consumer, never()).acknowledgementsForTransaction();

        batch.acknowledgementOwner().acknowledge(secondRecord, AcknowledgeType.REJECT);

        assertThat(batch.acknowledgementOwner().acknowledgementsForTransaction(), sameInstance(acknowledgements));
        verify(consumer).acknowledgementsForTransaction();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldAllowNextPollAfterReleaseButRejectTransactionalExtraction() {
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final Duration timeout = Duration.ofMillis(100);
        final ConsumerRecord<byte[], byte[]> record = record(1L);

        when(consumer.poll(timeout)).thenReturn(records(record), ConsumerRecords.empty());
        when(consumer.shareGroupMetadata()).thenReturn(new ShareGroupMetadata("share-group", "member", 3));

        final ShareSource source = new ShareSource(consumer);
        final ShareSource.PollResult batch = source.poll(timeout);
        batch.acknowledgementOwner().acknowledge(record, AcknowledgeType.RELEASE);

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> batch.acknowledgementOwner().acknowledgementsForTransaction()
        );

        assertThat(exception.getMessage(), is("Cannot extract transaction acknowledgements containing RELEASE decisions."));
        verify(consumer, never()).acknowledgementsForTransaction();

        source.poll(timeout);
        verify(consumer, times(2)).poll(timeout);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCloseShareConsumerWithSourceLifecycle() {
        final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
        final ShareSource source = new ShareSource(consumer);

        source.close();

        verify(consumer).close();
    }

    private static ConsumerRecord<byte[], byte[]> record(final long offset) {
        return new ConsumerRecord<>("input", 0, offset, new byte[0], new byte[0]);
    }

    @SafeVarargs
    private static ConsumerRecords<byte[], byte[]> records(final ConsumerRecord<byte[], byte[]>... records) {
        return new ConsumerRecords<>(Map.of(new TopicPartition("input", 0), List.of(records)), Map.of());
    }
}
