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
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.TxnCompleteRequestData;
import org.apache.kafka.common.message.TxnCompleteResponseData;
import org.apache.kafka.common.message.TxnShareAcknowledgeRequestData;
import org.apache.kafka.common.message.TxnShareAcknowledgeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShareTransactionalRequestsTest {
    private static final short VERSION = 0;
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Uuid ACQUISITION_ID = Uuid.randomUuid();

    @Test
    void testStageRequestRoundTripPreservesAcquisitionIdentity() {
        TxnShareAcknowledgeRequest request = new TxnShareAcknowledgeRequest.Builder(stageData(), true).build();
        TxnShareAcknowledgeRequest parsed = assertInstanceOf(TxnShareAcknowledgeRequest.class,
            AbstractRequest.parseRequest(ApiKeys.TXN_SHARE_ACKNOWLEDGE, VERSION, request.serialize()).request);

        assertEquals(request.data(), parsed.data());
        assertEquals(ACQUISITION_ID, parsed.data().topics().iterator().next().partitions().iterator().next()
            .acknowledgementBatches().get(0).acquisitionId());
        assertEquals((short) 7, parsed.data().producerEpoch());
    }

    @Test
    void testStageErrorResponsePreservesAllRequestedPartitions() {
        TxnShareAcknowledgeRequest request = new TxnShareAcknowledgeRequest.Builder(stageData(), true).build();
        TxnShareAcknowledgeResponse response = request.getErrorResponse(23, new UnknownServerException("private detail"));

        assertEquals(23, response.throttleTimeMs());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error());
        assertEquals(Map.of(Errors.UNKNOWN_SERVER_ERROR, 3), response.errorCounts());
        assertEquals(TOPIC_ID, response.data().responses().iterator().next().topicId());
        assertEquals(List.of(0, 1), response.data().responses().iterator().next().partitions().stream()
            .map(TxnShareAcknowledgeResponseData.TxnShareAcknowledgePartitionResponse::partitionIndex).collect(Collectors.toList()));
        assertFalse(response.data().toString().contains("private detail"));
        assertEquals(response.data(), AbstractResponse.parseResponse(ApiKeys.TXN_SHARE_ACKNOWLEDGE,
            response.serialize(VERSION), VERSION).data());
        assertTrue(response.shouldClientThrottle(VERSION));
    }

    @Test
    void testStageResponseCountsTopLevelAndPartitionErrors() {
        TxnShareAcknowledgeResponseData data = new TxnShareAcknowledgeResponseData()
            .setErrorCode(Errors.NONE.code());
        data.responses().add(new TxnShareAcknowledgeResponseData.TxnShareAcknowledgeTopicResponse()
            .setTopicId(TOPIC_ID)
            .setPartitions(List.of(
                new TxnShareAcknowledgeResponseData.TxnShareAcknowledgePartitionResponse()
                    .setPartitionIndex(0).setErrorCode(Errors.INVALID_RECORD_STATE.code()),
                new TxnShareAcknowledgeResponseData.TxnShareAcknowledgePartitionResponse()
                    .setPartitionIndex(1).setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()))));

        assertEquals(Map.of(Errors.NONE, 1, Errors.INVALID_RECORD_STATE, 1, Errors.NOT_LEADER_OR_FOLLOWER, 1),
            new TxnShareAcknowledgeResponse(data).errorCounts());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompletionRoundTripPreservesOriginalProducerEpoch(boolean commit) {
        TxnCompleteRequestData data = new TxnCompleteRequestData().setTransactions(List.of(
            transaction((short) 7, commit), transaction((short) 8, !commit)));
        TxnCompleteRequest request = new TxnCompleteRequest.Builder(data, true).build();

        assertEquals(data, AbstractRequest.parseRequest(ApiKeys.TXN_COMPLETE, VERSION, request.serialize()).request.data());
        TxnCompleteResponse response = request.getErrorResponse(100, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Map.of(Errors.CLUSTER_AUTHORIZATION_FAILED, 3), response.errorCounts());
        assertEquals(List.of((short) 7, (short) 8), response.data().transactions().stream()
            .map(TxnCompleteResponseData.TxnCompleteTransactionResponse::producerEpoch).collect(Collectors.toList()));
        assertEquals(response.data(), AbstractResponse.parseResponse(ApiKeys.TXN_COMPLETE,
            response.serialize(VERSION), VERSION).data());
        assertEquals(0, response.throttleTimeMs());
        response.maybeSetThrottleTimeMs(50);
        assertEquals(0, response.throttleTimeMs());
    }

    @Test
    void testApisAreBrokerOnlyAndRequireUnstableVersionOptIn() {
        for (ApiKeys apiKey : List.of(ApiKeys.TXN_SHARE_ACKNOWLEDGE, ApiKeys.TXN_COMPLETE)) {
            assertEquals(EnumSet.of(ApiMessageType.ListenerType.BROKER), apiKey.messageType.listeners());
            assertTrue(apiKey.toApiVersion(false).isEmpty());
            assertEquals(VERSION, apiKey.latestVersion(true));
            assertEquals((short) 3, apiKey.requestHeaderVersion(VERSION));
            assertEquals((short) 1, apiKey.responseHeaderVersion(VERSION));
        }
        assertFalse(ApiKeys.TXN_SHARE_ACKNOWLEDGE.clusterAction);
        assertTrue(ApiKeys.TXN_COMPLETE.clusterAction);
        assertThrows(UnsupportedVersionException.class,
            () -> new TxnShareAcknowledgeRequest.Builder(stageData()).build());
        assertThrows(UnsupportedVersionException.class,
            () -> new TxnCompleteRequest.Builder(new TxnCompleteRequestData()).build());
        assertThrows(UnsupportedVersionException.class,
            () -> new TxnShareAcknowledgeRequest.Builder(stageData(), true).build((short) 1));
        assertThrows(UnsupportedVersionException.class,
            () -> new TxnCompleteRequest.Builder(new TxnCompleteRequestData(), true).build((short) 1));
    }

    private TxnShareAcknowledgeRequestData stageData() {
        TxnShareAcknowledgeRequestData data = new TxnShareAcknowledgeRequestData()
            .setTransactionalId("transactional-id")
            .setGroupId("share-group")
            .setProducerId(42L)
            .setProducerEpoch((short) 7)
            .setMemberId(Uuid.randomUuid().toString())
            .setShareSessionEpoch(9);
        TxnShareAcknowledgeRequestData.TxnAcknowledgeTopic topic =
            new TxnShareAcknowledgeRequestData.TxnAcknowledgeTopic().setTopicId(TOPIC_ID);
        for (int partition : List.of(0, 1)) {
            topic.partitions().add(new TxnShareAcknowledgeRequestData.TxnAcknowledgePartition()
                .setPartitionIndex(partition)
                .setAcknowledgementBatches(List.of(new TxnShareAcknowledgeRequestData.AcknowledgementBatch()
                    .setFirstOffset(10).setLastOffset(11).setAcquisitionId(ACQUISITION_ID)
                    .setAcknowledgeTypes(List.of((byte) 1, (byte) 3)))));
        }
        data.topics().add(topic);
        return data;
    }

    private TxnCompleteRequestData.TxnCompleteTransaction transaction(short epoch, boolean commit) {
        return new TxnCompleteRequestData.TxnCompleteTransaction()
            .setTransactionalId("transactional-id").setProducerId(42L)
            .setProducerEpoch(epoch).setTransactionResult(commit);
    }
}
