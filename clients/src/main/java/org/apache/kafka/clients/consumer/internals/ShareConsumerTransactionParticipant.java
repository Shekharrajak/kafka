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
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeTransactionalEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler;
import org.apache.kafka.clients.producer.internals.ShareTransactionParticipant;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

final class ShareConsumerTransactionParticipant implements ShareTransactionParticipant {
    private final ApplicationEventHandler eventHandler;
    private final Time time;
    private final ShareAcknowledgementEventHandler acknowledgementEventHandler;
    private final int timeoutMs;
    private Cohort cohort;
    private boolean closed;
    private long completedSequence = -1;

    ShareConsumerTransactionParticipant(ApplicationEventHandler eventHandler, ShareAcknowledgementEventHandler acknowledgementEventHandler, Time time, int timeoutMs) {
        this.eventHandler = eventHandler;
        this.acknowledgementEventHandler = acknowledgementEventHandler;
        this.time = time;
        this.timeoutMs = timeoutMs;
    }

    synchronized void add(TransactionContext transaction, TransactionalShareAcknowledgement acknowledgement, boolean unitComplete) {
        if (closed || transaction.sequence() <= completedSequence) {
            throw new IllegalStateException("The share acknowledgement transaction has already completed or the consumer is closed");
        }
        if (cohort == null) {
            cohort = new Cohort(transaction);
        } else if (cohort.transaction != transaction) {
            throw new IllegalStateException("A poll unit cannot span producer transactions");
        }
        cohort.acknowledgements.add(acknowledgement);
        cohort.unitComplete = unitComplete;
    }

    synchronized boolean hasPendingTransaction() {
        return cohort != null;
    }

    synchronized void close() {
        closed = true;
    }

    @Override
    public synchronized CompletableFuture<Void> stage(TransactionContext transaction) {
        if (closed || cohort == null || cohort.transaction != transaction || !cohort.unitComplete) {
            CompletableFuture<Void> failure = new CompletableFuture<>();
            failure.completeExceptionally(new KafkaException("Every record in the poll unit must be acknowledged before committing"));
            return failure;
        }
        ShareAcknowledgeTransactionalEvent event = new ShareAcknowledgeTransactionalEvent(
            transaction, cohort.acknowledgements, time.milliseconds() + timeoutMs);
        try {
            eventHandler.add(event);
        } catch (RuntimeException e) {
            event.future().completeExceptionally(e);
        }
        return event.future();
    }

    @Override
    public synchronized void complete(TransactionContext transaction, Outcome outcome, RuntimeException exception) {
        completedSequence = Math.max(completedSequence, transaction.sequence());
        if (cohort != null && cohort.transaction == transaction) {
            Map<TopicIdPartition, Acknowledgements> completed = new HashMap<>();
            for (TransactionalShareAcknowledgement acknowledgement : cohort.acknowledgements) {
                Acknowledgements partitionAcknowledgements = completed.computeIfAbsent(acknowledgement.partition(), ignored -> Acknowledgements.empty());
                partitionAcknowledgements.add(acknowledgement.offset(), AcknowledgeType.forId(acknowledgement.acknowledgeType()));
                acknowledgement.complete();
            }
            KafkaException error = outcome == Outcome.COMMITTED ? null :
                new KafkaException("The producer transaction completed with outcome " + outcome, exception);
            completed.values().forEach(acknowledgements -> acknowledgements.complete(error));
            cohort = null;
            acknowledgementEventHandler.add(new ShareAcknowledgementEvent(completed, false, Optional.empty()));
        }
    }

    private static final class Cohort {
        private final TransactionContext transaction;
        private final List<TransactionalShareAcknowledgement> acknowledgements = new ArrayList<>();
        private boolean unitComplete;

        private Cohort(TransactionContext transaction) {
            this.transaction = transaction;
        }
    }
}
