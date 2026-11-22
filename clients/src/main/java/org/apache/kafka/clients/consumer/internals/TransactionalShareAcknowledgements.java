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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.producer.internals.ShareTransactionParticipant.TransactionContext;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.message.TxnShareAcknowledgeRequestData;
import org.apache.kafka.common.message.TxnShareAcknowledgeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.requests.TxnShareAcknowledgeRequest;
import org.apache.kafka.common.requests.TxnShareAcknowledgeResponse;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class TransactionalShareAcknowledgements {
    private TransactionalShareAcknowledgements() {
    }

    static TxnShareAcknowledgeRequest.Builder buildRequest(TransactionContext transaction, String groupId,
                                                         Uuid memberId, ShareSessionHandler session,
                                                         Map<String, Uuid> topicIds,
                                                         List<TransactionalShareAcknowledgement> acknowledgements) {
        ShareRequestMetadata sessionMetadata = session.transactionalAcknowledgementMetadata();
        TxnShareAcknowledgeRequestData data = new TxnShareAcknowledgeRequestData()
            .setTransactionalId(transaction.transactionalId())
            .setProducerId(transaction.producerId())
            .setProducerEpoch(transaction.producerEpoch())
            .setGroupId(groupId)
            .setMemberId(memberId.toString())
            .setShareSessionEpoch(sessionMetadata.epoch());
        Map<Uuid, TxnShareAcknowledgeRequestData.TxnAcknowledgeTopic> topics = new LinkedHashMap<>();
        Map<TopicIdPartition, TxnShareAcknowledgeRequestData.TxnAcknowledgePartition> partitions = new LinkedHashMap<>();
        List<TransactionalShareAcknowledgement> sortedAcknowledgements = new ArrayList<>(acknowledgements);
        sortedAcknowledgements.sort(Comparator.comparing((TransactionalShareAcknowledgement acknowledgement) -> acknowledgement.partition().topicId())
            .thenComparingInt(acknowledgement -> acknowledgement.partition().partition())
            .thenComparingLong(TransactionalShareAcknowledgement::offset));
        for (TransactionalShareAcknowledgement acknowledgement : sortedAcknowledgements) {
            TopicIdPartition tip = acknowledgement.partition();
            Uuid currentTopicId = topicIds.get(tip.topic());
            if (!tip.topicId().equals(currentTopicId) || !tip.equals(session.sessionPartitionMap().get(tip.topicPartition()))) {
                throw new KafkaException("The acknowledged topic or share session has changed");
            }
            TxnShareAcknowledgeRequestData.TxnAcknowledgeTopic topic = topics.computeIfAbsent(tip.topicId(),
                ignored -> new TxnShareAcknowledgeRequestData.TxnAcknowledgeTopic().setTopicId(tip.topicId()));
            TxnShareAcknowledgeRequestData.TxnAcknowledgePartition partition = partitions.get(tip);
            if (partition == null) {
                partition = new TxnShareAcknowledgeRequestData.TxnAcknowledgePartition().setPartitionIndex(tip.partition());
                partitions.put(tip, partition);
                topic.partitions().add(partition);
            }
            appendTransactionalAcknowledgement(partition, acknowledgement);
        }
        data.topics().addAll(topics.values());
        return new TxnShareAcknowledgeRequest.Builder(data, true);
    }

    private static void appendTransactionalAcknowledgement(TxnShareAcknowledgeRequestData.TxnAcknowledgePartition partition,
                                                   TransactionalShareAcknowledgement acknowledgement) {
        List<TxnShareAcknowledgeRequestData.AcknowledgementBatch> batches = partition.acknowledgementBatches();
        TxnShareAcknowledgeRequestData.AcknowledgementBatch previous = batches.isEmpty() ? null : batches.get(batches.size() - 1);
        if (previous != null && previous.lastOffset() + 1 == acknowledgement.offset()
            && previous.acquisitionId().equals(acknowledgement.acquisitionId())
            && previous.acknowledgeTypes().get(0) == acknowledgement.acknowledgeType()) {
            previous.setLastOffset(acknowledgement.offset());
        } else {
            batches.add(new TxnShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(acknowledgement.offset())
                .setLastOffset(acknowledgement.offset())
                .setAcquisitionId(acknowledgement.acquisitionId())
                .setAcknowledgeTypes(List.of(acknowledgement.acknowledgeType())));
        }
    }

    static void handleResponse(ShareSessionHandler session, ClientResponse clientResponse,
                               List<TransactionalShareAcknowledgement> acknowledgements) {
        TxnShareAcknowledgeResponse response = (TxnShareAcknowledgeResponse) clientResponse.responseBody();
        session.handleResponse(response);
        Errors topLevelError = Errors.forCode(response.data().errorCode());
        if (topLevelError != Errors.NONE) {
            throw topLevelError.exception(response.data().errorMessage());
        }
        Map<Uuid, Set<Integer>> expected = new HashMap<>();
        for (TransactionalShareAcknowledgement acknowledgement : acknowledgements) {
            expected.computeIfAbsent(acknowledgement.partition().topicId(), ignored -> new HashSet<>())
                .add(acknowledgement.partition().partition());
        }
        for (TxnShareAcknowledgeResponseData.TxnShareAcknowledgeTopicResponse topic : response.data().responses()) {
            for (TxnShareAcknowledgeResponseData.TxnShareAcknowledgePartitionResponse partition : topic.partitions()) {
                Set<Integer> expectedPartitions = expected.get(topic.topicId());
                if (expectedPartitions == null || !expectedPartitions.remove(partition.partitionIndex())) {
                    throw invalidResponse();
                }
                Errors partitionError = Errors.forCode(partition.errorCode());
                if (partitionError != Errors.NONE) {
                    throw partitionError.exception(partition.errorMessage());
                }
            }
        }
        if (expected.values().stream().anyMatch(partitions -> !partitions.isEmpty())) {
            throw invalidResponse();
        }
    }

    private static InvalidRecordStateException invalidResponse() {
        return new InvalidRecordStateException("Transactional acknowledgement not successful due to invalid response from broker");
    }
}
