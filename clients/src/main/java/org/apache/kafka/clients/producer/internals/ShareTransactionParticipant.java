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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.annotation.InterfaceAudience;

import java.util.concurrent.CompletableFuture;

@InterfaceAudience.Private
public interface ShareTransactionParticipant {
    /** Enqueues staging without blocking the producer network thread. */
    CompletableFuture<Void> stage(TransactionContext transaction);

    /** Enqueues outcome notification without blocking the producer network thread. */
    void complete(TransactionContext transaction, Outcome outcome, RuntimeException exception);

    enum Outcome {
        COMMITTED,
        ABORTED,
        UNKNOWN
    }

    interface Registration extends AutoCloseable {
        Reservation reserve();

        @Override
        void close();
    }

    interface Reservation {
        TransactionContext transaction();

        void complete();

        void fail(RuntimeException exception);
    }

    final class TransactionContext {
        private final String transactionalId;
        private final long producerId;
        private final short producerEpoch;
        private final long sequence;

        TransactionContext(String transactionalId, long producerId, short producerEpoch, long sequence) {
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.sequence = sequence;
        }

        public String transactionalId() {
            return transactionalId;
        }

        public long producerId() {
            return producerId;
        }

        public short producerEpoch() {
            return producerEpoch;
        }

        public long sequence() {
            return sequence;
        }
    }
}
