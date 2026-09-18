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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Objects;
import java.util.Optional;

final class ShareIngressDelivery {
    private final TopicPartition ingressPartition;
    private final long ingressOffset;
    private final Optional<Integer> ingressLeaderEpoch;
    private final ShareIngressRecord sourceRecord;

    private ShareIngressDelivery(final TopicPartition ingressPartition,
                                 final long ingressOffset,
                                 final Optional<Integer> ingressLeaderEpoch,
                                 final ShareIngressRecord sourceRecord) {
        this.ingressPartition = ingressPartition;
        this.ingressOffset = ingressOffset;
        this.ingressLeaderEpoch = ingressLeaderEpoch;
        this.sourceRecord = sourceRecord;
    }

    static ShareIngressDelivery decode(final TaskId taskId,
                                       final ConsumerRecord<byte[], byte[]> ingressRecord,
                                       final ShareIngressAssignment assignment) {
        Objects.requireNonNull(taskId, "taskId cannot be null");
        Objects.requireNonNull(ingressRecord, "ingressRecord cannot be null");
        Objects.requireNonNull(assignment, "assignment cannot be null");
        final TopicPartition ingressPartition = new TopicPartition(ingressRecord.topic(), ingressRecord.partition());
        if (!taskId.equals(assignment.targetTaskForIngress(ingressPartition))) {
            throw new IllegalArgumentException("Ingress partition " + ingressPartition + " is not assigned to task " + taskId);
        }
        final ShareIngressRecord sourceRecord = ShareIngressRecordSerde.deserialize(taskId, ingressRecord.value());
        final TopicPartition sourcePartition = new TopicPartition(sourceRecord.topic(), sourceRecord.partition());
        if (!sourcePartition.equals(assignment.sourcePartitionForIngress(ingressPartition))) {
            throw new IllegalArgumentException("Ingress partition " + ingressPartition + " does not match source partition " + sourcePartition);
        }
        return new ShareIngressDelivery(ingressPartition, ingressRecord.offset(), ingressRecord.leaderEpoch(), sourceRecord);
    }

    TopicPartition ingressPartition() {
        return ingressPartition;
    }

    long ingressOffset() {
        return ingressOffset;
    }

    Optional<Integer> ingressLeaderEpoch() {
        return ingressLeaderEpoch;
    }

    ShareIngressRecord sourceRecord() {
        return sourceRecord;
    }
}
