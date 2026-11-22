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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class TransactionalShareAcknowledgement {
    private final int nodeId;
    private final Uuid memberId;
    private final TopicIdPartition partition;
    private final long offset;
    private final Uuid acquisitionId;
    private final byte acknowledgeType;
    private final AtomicInteger pendingAcknowledgements;
    private final AtomicBoolean retained = new AtomicBoolean();

    TransactionalShareAcknowledgement(int nodeId, Uuid memberId, TopicIdPartition partition, long offset,
                                     Uuid acquisitionId, byte acknowledgeType, AtomicInteger pendingAcknowledgements) {
        this.nodeId = nodeId;
        this.memberId = memberId;
        this.partition = partition;
        this.offset = offset;
        this.acquisitionId = acquisitionId;
        this.acknowledgeType = acknowledgeType;
        this.pendingAcknowledgements = pendingAcknowledgements;
    }

    public int nodeId() {
        return nodeId;
    }

    public Uuid memberId() {
        return memberId;
    }

    public TopicIdPartition partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public Uuid acquisitionId() {
        return acquisitionId;
    }

    public byte acknowledgeType() {
        return acknowledgeType;
    }

    void retain() {
        retained.set(true);
    }

    void complete() {
        if (retained.compareAndSet(true, false)) {
            pendingAcknowledgements.decrementAndGet();
        }
    }
}
