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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransactionalShareStateMessageTest {
    @Test
    void fencedReadCannotDowngradeToAnOrdinaryRead() {
        Uuid topicId = Uuid.randomUuid();
        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId("group")
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(topicId)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(3)
                    .setLeaderEpoch(5)
                    .setExpectedWriterGeneration(Uuid.randomUuid())
                    .setNewWriterGeneration(Uuid.randomUuid())))));

        assertThrows(UnsupportedVersionException.class, () -> MessageUtil.toByteBufferAccessor(request, (short) 1));
        assertEquals(request, new ReadShareGroupStateRequestData(
            MessageUtil.toByteBufferAccessor(request, (short) 2), (short) 2));
    }

    @Test
    void transactionalWriteCannotDowngradeToAnOrdinaryWrite() {
        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId("group")
            .setTransactionalId("task-1")
            .setProducerId(5)
            .setProducerEpoch((short) 7);

        assertThrows(UnsupportedVersionException.class, () -> MessageUtil.toByteBufferAccessor(request, (short) 2));
        assertEquals(request, new WriteShareGroupStateRequestData(
            MessageUtil.toByteBufferAccessor(request, (short) 3), (short) 3));
    }

    @Test
    void pendingStateCannotBeReturnedAsOrdinaryState() {
        var pending = new ReadShareGroupStateResponseData.PendingTransaction()
            .setTransactionalId("task-1")
            .setProducerId(5)
            .setProducerEpoch((short) 7)
            .setStateBatches(List.of(new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(42)
                .setLastOffset(42)
                .setDeliveryState((byte) 2)
                .setDeliveryCount((short) 1)));
        ReadShareGroupStateResponseData response = new ReadShareGroupStateResponseData()
            .setResults(List.of(new ReadShareGroupStateResponseData.ReadStateResult()
                .setTopicId(Uuid.randomUuid())
                .setPartitions(List.of(new ReadShareGroupStateResponseData.PartitionResult()
                    .setStateIncarnationId(Uuid.randomUuid())
                    .setWriterGeneration(Uuid.randomUuid())
                    .setPendingTransactions(List.of(pending))))));

        assertThrows(UnsupportedVersionException.class, () -> MessageUtil.toByteBufferAccessor(response, (short) 1));
        assertEquals(response, new ReadShareGroupStateResponseData(
            MessageUtil.toByteBufferAccessor(response, (short) 2), (short) 2));
    }
}
