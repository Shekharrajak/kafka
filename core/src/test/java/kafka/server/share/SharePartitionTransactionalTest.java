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
package kafka.server.share;

import kafka.server.share.SharePartition.SharePartitionState;
import kafka.server.share.SharePartitionManager.SharePartitionListener;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.internals.ShareAcquireMode;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.message.TxnShareAcknowledgeRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigProvider;
import org.apache.kafka.server.share.PartitionMetadataProvider;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.dlq.ShareGroupDLQManager;
import org.apache.kafka.server.share.fetch.InFlightBatch;
import org.apache.kafka.server.share.fetch.RecordState;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.metrics.SharePartitionMetrics;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.memoryRecordsBuilder;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SharePartitionTransactionalTest {
    private static final String GROUP = "group";
    private static final String MEMBER = "member";
    private static final String TRANSACTION = "transaction";
    private static final TopicIdPartition PARTITION = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("input", 0));
    private final Persister persister = mock(Persister.class);
    private final ShareGroupDLQManager dlq = mock(ShareGroupDLQManager.class);
    private final PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
    private final MockTimer timer = new MockTimer();
    private final AtomicReference<ReadShareGroupStateResponseData.PartitionResult> persisted = new AtomicReference<>();
    private SharePartition sharePartition;

    @BeforeEach
    void setUp() {
        persisted.set(new ReadShareGroupStateResponseData.PartitionResult().setPartition(0).setStateEpoch(1)
            .setStateIncarnationId(Uuid.randomUuid()).setStartOffset(10));
        when(persister.readFencedState(any(), anyInt(), any(), any())).thenAnswer(invocation -> {
            Uuid expected = invocation.getArgument(2);
            Uuid generation = invocation.getArgument(3);
            ReadShareGroupStateResponseData.PartitionResult state = persisted.get();
            if (!generation.equals(Uuid.ZERO_UUID)) {
                if (!expected.equals(state.writerGeneration())) {
                    return CompletableFuture.completedFuture(state.duplicate().setErrorCode(Errors.FENCED_STATE_EPOCH.code()));
                }
                state.setWriterGeneration(generation);
            }
            return CompletableFuture.completedFuture(state.duplicate());
        });
        when(persister.writeFencedState(any(), any(), any(), anyLong(), anyShort()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(dlq.enqueue(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(metadataProvider.offsetForLatestTimestamp(PARTITION, 0)).thenReturn(10L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (sharePartition != null) {
            sharePartition.markFenced();
            sharePartition.close();
        }
        timer.close();
    }

    @Test
    void testDuplicateStageWaitsForOriginalDurabilityAndDoesNotFinalize() {
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        assertNotEquals(Uuid.ZERO_UUID, acquired.acquisitionId());
        CompletableFuture<Void> write = new CompletableFuture<>();
        when(persister.writeFencedState(any(), any(), eq(TRANSACTION), eq(42L), eq((short) 7))).thenReturn(write);
        List<TxnShareAcknowledgeRequestData.AcknowledgementBatch> batches = acknowledgements(acquired, (byte) 1);

        CompletableFuture<Void> first = stage(batches);
        CompletableFuture<Void> retry = stage(batches);
        assertFalse(first.isDone());
        assertFalse(retry.isDone());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
        assertTrue(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(0, acquire().count());
        write.complete(null);
        first.join();
        retry.join();

        assertEquals(10, sharePartition.startOffset());
        assertTrue(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
        ArgumentCaptor<WriteShareGroupStateRequestData.PartitionData> data =
            ArgumentCaptor.forClass(WriteShareGroupStateRequestData.PartitionData.class);
        verify(persister).writeFencedState(any(), data.capture(), eq(TRANSACTION), eq(42L), eq((short) 7));
        assertEquals(-1, data.getValue().startOffset());
        assertEquals(-1, data.getValue().deliveryCompleteCount());
        assertEquals(persisted.get().writerGeneration(), data.getValue().writerGeneration());
    }

    @Test
    void testUncertainStageFailureFencesCacheAndEveryDuplicateFails() {
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        CompletableFuture<Void> write = new CompletableFuture<>();
        when(persister.writeFencedState(any(), any(), eq(TRANSACTION), eq(42L), eq((short) 7))).thenReturn(write);
        CompletableFuture<Void> first = stage(acknowledgements(acquired, (byte) 1));
        CompletableFuture<Void> retry = stage(acknowledgements(acquired, (byte) 1));
        write.completeExceptionally(Errors.REQUEST_TIMED_OUT.exception());

        assertFutureThrows(NotLeaderOrFollowerException.class, first);
        assertFutureThrows(NotLeaderOrFollowerException.class, retry);
        assertEquals(SharePartitionState.FENCED, sharePartition.partitionState());
        assertEquals(0, acquire().count());
        assertTrue(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
    }

    @Test
    void testSameMemberReacquisitionRejectsOldAcquisitionId() {
        initialize();
        AcquiredRecords first = acquire().acquiredRecords().get(0);
        sharePartition.acknowledge(MEMBER, List.of(new ShareAcknowledgementBatch(10, 14,
            List.of(AcknowledgeType.RELEASE.id)))).join();
        AcquiredRecords second = acquire().acquiredRecords().get(0);

        assertNotEquals(first.acquisitionId(), second.acquisitionId());
        assertEquals(2, second.deliveryCount());
        assertFutureThrows(InvalidRecordStateException.class, stage(acknowledgements(first, (byte) 1)));
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        verify(persister, never()).writeFencedState(any(), any(), eq(TRANSACTION), anyLong(), anyShort());
    }

    @Test
    void testCommittedReadResolvesOnlyHeldOffsetsAndLostHintIsRecoverable() {
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        stage(List.of(new TxnShareAcknowledgeRequestData.AcknowledgementBatch().setFirstOffset(10).setLastOffset(10)
            .setAcquisitionId(acquired.acquisitionId()).setAcknowledgeTypes(List.of((byte) 1)))).join();
        persisted.get().setStateBatches(List.of(stateBatch(10, 10, RecordState.ACKNOWLEDGED, 1),
            stateBatch(11, 14, RecordState.AVAILABLE, 0)));

        sharePartition.reconcileTransactionalAcknowledgements();

        assertEquals(11, sharePartition.startOffset());
        InFlightBatch batch = sharePartition.cachedState().get(10L);
        for (long offset = 11; offset <= 14; offset++) {
            assertEquals(RecordState.ACQUIRED, batch.offsetState().get(offset).state());
            assertEquals(MEMBER, batch.offsetState().get(offset).memberId());
            assertEquals(acquired.acquisitionId(), batch.offsetState().get(offset).acquisitionId());
        }
    }

    @Test
    void testAbsenceWithoutCoveringCommittedStateDoesNotReleaseHeldInput() {
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        stage(acknowledgements(acquired, (byte) 1)).join();
        sharePartition.reconcileTransactionalAcknowledgements();

        assertEquals(10, sharePartition.startOffset());
        assertTrue(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
        assertEquals(0, acquire().count());
    }

    @Test
    void testRestoredRejectDoesNotStartDlqUntilCommittedStateIsObserved() {
        persisted.get().setStateBatches(List.of(stateBatch(10, 14, RecordState.AVAILABLE, 1)))
            .setPendingTransactions(List.of(new ReadShareGroupStateResponseData.PendingTransaction()
                .setTransactionalId(TRANSACTION).setProducerId(42).setProducerEpoch((short) 7)
                .setStateBatches(List.of(stateBatch(10, 14, RecordState.ARCHIVING, 1)))));
        initialize();

        verify(dlq, never()).enqueue(any());
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(10, sharePartition.startOffset());
        assertTrue(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
        persisted.get().setPendingTransactions(List.of())
            .setStateBatches(List.of(stateBatch(10, 14, RecordState.ARCHIVING, 1)));
        sharePartition.reconcileTransactionalAcknowledgements();

        verify(dlq).enqueue(any());
        assertEquals(15, sharePartition.startOffset());
        assertTrue(sharePartition.cachedState().isEmpty());
    }

    @Test
    void testAbortAfterExpiredLeaseAtDeliveryLimitRestoresAvailable() throws Exception {
        persisted.get().setStateBatches(List.of(stateBatch(10, 14, RecordState.AVAILABLE, 4)));
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        assertEquals(5, acquired.deliveryCount());
        stage(acknowledgements(acquired, (byte) 1)).join();
        persisted.get().setPendingTransactions(List.of(new ReadShareGroupStateResponseData.PendingTransaction()
            .setTransactionalId(TRANSACTION).setProducerId(42).setProducerEpoch((short) 7)
            .setStateBatches(List.of(stateBatch(acquired.firstOffset(), acquired.lastOffset(), RecordState.ACKNOWLEDGED, 5)))));
        timer.advanceClock(31000);
        assertTrue(sharePartition.cachedState().get(10L).offsetState().get(10L).hasOngoingStateTransition());
        persisted.get().setPendingTransactions(List.of())
            .setStateBatches(List.of(stateBatch(acquired.firstOffset(), acquired.lastOffset(), RecordState.AVAILABLE, 5)));
        sharePartition.reconcileTransactionalAcknowledgements();

        InFlightBatch batch = sharePartition.cachedState().get(10L);
        assertEquals(RecordState.AVAILABLE, batch.offsetState().get(10L).state());
        assertEquals(5, batch.offsetState().get(10L).deliveryCount());
        assertEquals(0, sharePartition.deliveryCompleteCount());
        verify(dlq, never()).enqueue(any());
    }

    @Test
    void testReconciliationCannotUseReadIssuedBeforeStageIsDurable() {
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        CompletableFuture<Void> write = new CompletableFuture<>();
        when(persister.writeFencedState(any(), any(), eq(TRANSACTION), eq(42L), eq((short) 7))).thenReturn(write);
        stage(acknowledgements(acquired, (byte) 1));
        persisted.get().setStateBatches(List.of(stateBatch(10, 14, RecordState.AVAILABLE, 0)));

        sharePartition.reconcileTransactionalAcknowledgements();
        write.complete(null);

        assertTrue(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
        verify(persister, times(2)).readFencedState(any(), anyInt(), any(), any());
    }

    @Test
    void testReadFromSupersededWriterFencesHeldState() {
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        stage(acknowledgements(acquired, (byte) 1)).join();
        persisted.get().setWriterGeneration(Uuid.randomUuid())
            .setStateBatches(List.of(stateBatch(10, 14, RecordState.AVAILABLE, 1)));

        sharePartition.reconcileTransactionalAcknowledgements();

        assertEquals(SharePartitionState.FENCED, sharePartition.partitionState());
        assertTrue(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
    }

    @Test
    void testInvalidLaterRangeDoesNotPoisonEarlierAcquisition() {
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        List<TxnShareAcknowledgeRequestData.AcknowledgementBatch> batches = List.of(
            new TxnShareAcknowledgeRequestData.AcknowledgementBatch().setFirstOffset(10).setLastOffset(10)
                .setAcquisitionId(acquired.acquisitionId()).setAcknowledgeTypes(List.of((byte) 1)),
            new TxnShareAcknowledgeRequestData.AcknowledgementBatch().setFirstOffset(11).setLastOffset(11)
                .setAcquisitionId(Uuid.randomUuid()).setAcknowledgeTypes(List.of((byte) 1)));

        assertFutureThrows(InvalidRecordStateException.class, stage(batches));

        InFlightBatch batch = sharePartition.cachedState().get(10L);
        for (long offset = 10; offset <= 14; offset++) {
            assertEquals(RecordState.ACQUIRED, batch.offsetState().get(offset).state());
            assertFalse(batch.offsetState().get(offset).hasOngoingStateTransition());
        }
        verify(persister, never()).writeFencedState(any(), any(), eq(TRANSACTION), anyLong(), anyShort());
    }

    @Test
    void testLogStartMovementDoesNotSubtractPendingTerminalRecords() {
        initialize();
        AcquiredRecords acquired = acquire().acquiredRecords().get(0);
        stage(acknowledgements(acquired, (byte) 1)).join();

        sharePartition.updateCacheAndOffsets(12);

        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertTrue(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
    }

    @Test
    void testInitialAutoResetFrontierIsDurableBeforeAcquisition() {
        persisted.get().setStartOffset(-1);
        CompletableFuture<Void> baseline = new CompletableFuture<>();
        when(persister.writeFencedState(any(), any(), isNull(), eq(-1L), eq((short) -1))).thenReturn(baseline);
        createSharePartition();

        CompletableFuture<Void> initialized = sharePartition.maybeInitialize();

        assertFalse(initialized.isDone());
        assertEquals(SharePartitionState.INITIALIZING, sharePartition.partitionState());
        assertEquals(0, acquire().count());
        ArgumentCaptor<WriteShareGroupStateRequestData.PartitionData> data =
            ArgumentCaptor.forClass(WriteShareGroupStateRequestData.PartitionData.class);
        verify(persister).writeFencedState(any(), data.capture(), isNull(), eq(-1L), eq((short) -1));
        assertEquals(10, data.getValue().startOffset());
        assertTrue(data.getValue().stateBatches().isEmpty());
        baseline.complete(null);
        initialized.join();
        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
    }

    @Test
    void testRestoreRejectsPendingRangeBeforePersistedFrontier() {
        persisted.get().setStartOffset(12).setPendingTransactions(List.of(new ReadShareGroupStateResponseData.PendingTransaction()
            .setTransactionalId(TRANSACTION).setProducerId(42).setProducerEpoch((short) 7)
            .setStateBatches(List.of(stateBatch(10, 14, RecordState.ACKNOWLEDGED, 1)))));
        createSharePartition();

        assertFutureThrows(IllegalStateException.class, sharePartition.maybeInitialize());

        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
        assertEquals(0, acquire().count());
    }

    @Test
    void testAcquiredRangeCoalescingPreservesDifferentIdentities() {
        Uuid first = Uuid.randomUuid();
        Uuid second = Uuid.randomUuid();
        List<AcquiredRecords> result = new ArrayList<>();
        ShareFetchUtils.accumulateAcquiredRecords(result, List.of(
            new AcquiredRecords().setFirstOffset(10).setLastOffset(10).setDeliveryCount((short) 1).setAcquisitionId(first),
            new AcquiredRecords().setFirstOffset(11).setLastOffset(11).setDeliveryCount((short) 1).setAcquisitionId(second),
            new AcquiredRecords().setFirstOffset(12).setLastOffset(12).setDeliveryCount((short) 1).setAcquisitionId(second)));

        assertEquals(2, result.size());
        assertEquals(first, result.get(0).acquisitionId());
        assertEquals(11, result.get(1).firstOffset());
        assertEquals(12, result.get(1).lastOffset());
        assertEquals(second, result.get(1).acquisitionId());
    }

    private void initialize() {
        createSharePartition();
        sharePartition.maybeInitialize().join();
    }

    private void createSharePartition() {
        sharePartition = new SharePartition(GROUP, PARTITION, 0, 100, 5, 30000, timer, timer.time(), persister,
            metadataProvider, ignored -> { },
            new ShareGroupConfigProvider(mock(GroupConfigManager.class)), SharePartitionState.EMPTY,
            mock(SharePartitionListener.class), mock(SharePartitionMetrics.class), () -> false, dlq, () -> true);
    }

    private ShareAcquiredRecords acquire() {
        MemoryRecords records;
        try (MemoryRecordsBuilder builder = memoryRecordsBuilder(10L, 5)) {
            records = builder.build();
        }
        return sharePartition.acquire(MEMBER, ShareAcquireMode.RECORD_LIMIT, 100, 5, 10,
            new FetchPartitionData(Errors.NONE, 15, 0, records, Optional.empty(), OptionalLong.empty(),
                Optional.empty(), OptionalInt.empty(), false), FetchIsolation.HIGH_WATERMARK);
    }

    private CompletableFuture<Void> stage(List<TxnShareAcknowledgeRequestData.AcknowledgementBatch> batches) {
        return sharePartition.acknowledgeTransactional(MEMBER, TRANSACTION, 42L, (short) 7, batches);
    }

    private List<TxnShareAcknowledgeRequestData.AcknowledgementBatch> acknowledgements(AcquiredRecords acquired, byte type) {
        return List.of(new TxnShareAcknowledgeRequestData.AcknowledgementBatch()
            .setFirstOffset(acquired.firstOffset()).setLastOffset(acquired.lastOffset())
            .setAcquisitionId(acquired.acquisitionId()).setAcknowledgeTypes(List.of(type)));
    }

    private ReadShareGroupStateResponseData.StateBatch stateBatch(long first, long last, RecordState state, int count) {
        return new ReadShareGroupStateResponseData.StateBatch().setFirstOffset(first).setLastOffset(last)
            .setDeliveryState(state.id()).setDeliveryCount((short) count);
    }
}
