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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeTransactionalEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler;
import org.apache.kafka.clients.producer.internals.ShareTransactionParticipant;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareConsumerTransactionParticipantTest {
    private final ApplicationEventHandler events = mock(ApplicationEventHandler.class);
    private final LinkedBlockingQueue<ShareAcknowledgementEvent> completed = new LinkedBlockingQueue<>();
    private final ShareConsumerTransactionParticipant participant = new ShareConsumerTransactionParticipant(
        events, new ShareAcknowledgementEventHandler(completed), new MockTime(), 1000);
    private final TopicIdPartition partition = new TopicIdPartition(Uuid.randomUuid(), 0, "input");

    @Test
    public void testAbortBeforeAcknowledgementPublicationRejectsStaleCohort() {
        ShareTransactionParticipant.TransactionContext aborted = transaction(1);
        participant.complete(aborted, ShareTransactionParticipant.Outcome.ABORTED, null);
        assertThrows(IllegalStateException.class, () -> participant.add(aborted, acknowledgement(0), true));
        assertFalse(participant.hasPendingTransaction());
        participant.add(transaction(2), acknowledgement(1), true);
        assertTrue(participant.hasPendingTransaction());
    }

    @Test
    public void testIncompletePollUnitCannotStage() {
        ShareTransactionParticipant.TransactionContext transaction = transaction(1);
        participant.add(transaction, acknowledgement(0), false);
        assertTrue(participant.stage(transaction).isCompletedExceptionally());
        verify(events, never()).add(any());
        participant.complete(transaction, ShareTransactionParticipant.Outcome.ABORTED, null);
        assertFalse(participant.hasPendingTransaction());
        assertNotNull(completed.peek().acknowledgementsMap().get(partition).getAcknowledgeException());
    }

    @Test
    public void testStageDoesNotCompleteAcknowledgementsBeforeFinalTransactionOutcome() {
        ShareTransactionParticipant.TransactionContext transaction = transaction(1);
        participant.add(transaction, acknowledgement(0), true);
        participant.stage(transaction);
        ArgumentCaptor<ShareAcknowledgeTransactionalEvent> event = ArgumentCaptor.forClass(ShareAcknowledgeTransactionalEvent.class);
        verify(events).add(event.capture());
        event.getValue().future().complete(null);
        assertTrue(participant.hasPendingTransaction());
        assertTrue(completed.isEmpty());
        participant.complete(transaction, ShareTransactionParticipant.Outcome.COMMITTED, null);
        assertFalse(participant.hasPendingTransaction());
        assertEquals(1, completed.size());
        assertNull(completed.peek().acknowledgementsMap().get(partition).getAcknowledgeException());
        participant.complete(transaction, ShareTransactionParticipant.Outcome.COMMITTED, null);
        assertEquals(1, completed.size());
    }

    private ShareTransactionParticipant.TransactionContext transaction(long sequence) {
        ShareTransactionParticipant.TransactionContext transaction = mock(ShareTransactionParticipant.TransactionContext.class);
        when(transaction.sequence()).thenReturn(sequence);
        return transaction;
    }

    private TransactionalShareAcknowledgement acknowledgement(long offset) {
        Uuid memberId = Uuid.randomUuid();
        ShareInFlightBatch<String, String> owner = new ShareInFlightBatch<>(0, partition, Optional.empty(), memberId);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(partition.topic(), partition.partition(), offset, "key", "value");
        owner.addRecord(record, Uuid.randomUuid());
        TransactionalShareAcknowledgement acknowledgement = owner.transactionalAcknowledgement(record, AcknowledgeType.ACCEPT);
        owner.removeTransactionalRecord(record, acknowledgement);
        return acknowledgement;
    }
}
