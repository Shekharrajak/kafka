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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

final class ShareTransactionState {
    record Owner(long producerId, short producerEpoch) { }
    record Key(Owner owner, SharePartitionKey sharePartition) { }
    record Intent(String transactionalId, Uuid incarnationId, List<PersisterStateBatch> batches) {
        Intent {
            batches = List.copyOf(batches);
        }
    }
    private record Pin(long offset, boolean resolved, String transactionalId) { }

    private final TimelineHashMap<Key, Intent> pending;
    private final TimelineHashMap<Key, Pin> pins;

    ShareTransactionState(SnapshotRegistry registry) {
        pending = new TimelineHashMap<>(registry, 0);
        pins = new TimelineHashMap<>(registry, 0);
    }

    boolean hasPending(SharePartitionKey key) {
        return pending.keySet().stream().anyMatch(k -> k.sharePartition().equals(key));
    }

    Errors validate(SharePartitionKey key, Owner owner, String transactionalId,
                    List<PersisterStateBatch> batches, long startOffset) {
        if (owner != null && hasConflictingBatches(batches)) return Errors.INVALID_REQUEST;
        for (Map.Entry<Key, Intent> entry : pending.entrySet()) {
            if (!entry.getKey().sharePartition().equals(key)) continue;
            Errors error = validateIntent(entry.getKey().owner(), entry.getValue(), owner, transactionalId, batches, startOffset);
            if (error != Errors.NONE) return error;
        }
        return Errors.NONE;
    }

    private static boolean hasConflictingBatches(List<PersisterStateBatch> batches) {
        for (int i = 0; i < batches.size(); i++) {
            for (int j = i + 1; j < batches.size(); j++) {
                PersisterStateBatch a = batches.get(i);
                PersisterStateBatch b = batches.get(j);
                if (overlaps(a, b) && !sameAcknowledgement(a, b)) return true;
            }
        }
        return false;
    }

    private static Errors validateIntent(Owner heldOwner, Intent intent, Owner owner, String transactionalId,
                                         List<PersisterStateBatch> batches, long startOffset) {
        boolean sameOwner = heldOwner.equals(owner);
        if (sameOwner && !intent.transactionalId().equals(transactionalId)) return Errors.INVALID_REQUEST;
        for (PersisterStateBatch held : intent.batches()) {
            if (startOffset > held.firstOffset()) return Errors.INVALID_RECORD_STATE;
            if (batches.stream().anyMatch(batch -> overlaps(held, batch) &&
                    (!sameOwner || !sameAcknowledgement(held, batch)))) return Errors.INVALID_RECORD_STATE;
        }
        return Errors.NONE;
    }

    private static boolean sameAcknowledgement(PersisterStateBatch a, PersisterStateBatch b) {
        return a.deliveryState() == b.deliveryState() && a.deliveryCount() == b.deliveryCount();
    }

    boolean isDuplicate(Key key, List<PersisterStateBatch> batches) {
        Intent intent = pending.get(key);
        if (intent == null) return false;
        for (PersisterStateBatch batch : batches) {
            long next = batch.firstOffset();
            boolean covered = false;
            for (PersisterStateBatch held : intent.batches()) {
                if (held.lastOffset() < next) continue;
                if (held.firstOffset() > next || held.deliveryState() != batch.deliveryState() ||
                        held.deliveryCount() != batch.deliveryCount()) break;
                if (held.lastOffset() >= batch.lastOffset()) {
                    covered = true;
                    break;
                }
                next = held.lastOffset() + 1;
            }
            if (!covered) return false;
        }
        return true;
    }

    void replay(long offset, Key key, ShareUpdateValue value) {
        Intent previous = pending.get(key);
        List<PersisterStateBatch> incoming = value.stateBatches().stream()
            .map(batch -> new PersisterStateBatch(batch.firstOffset(), batch.lastOffset(),
                batch.deliveryState(), batch.deliveryCount()))
            .toList();
        if (previous != null && (!previous.transactionalId().equals(value.transactionalId()) ||
                !previous.incarnationId().equals(value.stateIncarnationId()))) {
            throw new IllegalStateException("Conflicting share transaction identity during replay");
        }
        List<PersisterStateBatch> merged = new PersisterStateBatchCombiner(
            previous == null ? List.of() : previous.batches(), incoming, 0).combineStateBatches();
        pending.put(key, new Intent(value.transactionalId(), value.stateIncarnationId(), merged));
        Pin pin = pins.get(key);
        pins.put(key, new Pin(pin == null ? offset : Math.min(pin.offset(), offset), false, value.transactionalId()));
    }

    Map<SharePartitionKey, Intent> complete(Owner owner) {
        List<Key> keys = pending.keySet().stream().filter(k -> k.owner().equals(owner)).toList();
        Map<SharePartitionKey, Intent> result = keys.stream().collect(Collectors.toMap(
            Key::sharePartition, pending::get));
        for (Key key : keys) {
            pending.remove(key);
            Pin pin = pins.get(key);
            pins.put(key, new Pin(pin.offset(), true, pin.transactionalId()));
        }
        return result;
    }

    void snapshot(SharePartitionKey key) {
        pins.entrySet().stream().filter(e -> e.getKey().sharePartition().equals(key) && e.getValue().resolved())
            .map(Map.Entry::getKey).toList().forEach(pins::remove);
    }

    void delete(SharePartitionKey key) {
        pending.keySet().stream().filter(k -> k.sharePartition().equals(key)).toList().forEach(pending::remove);
        pins.keySet().stream().filter(k -> k.sharePartition().equals(key)).toList().forEach(pins::remove);
    }

    Set<SharePartitionKey> unresolvedSnapshots() {
        return pins.entrySet().stream().filter(e -> e.getValue().resolved())
            .map(e -> e.getKey().sharePartition()).collect(Collectors.toSet());
    }

    OptionalLong firstRequiredOffset() {
        return pins.values().stream().mapToLong(Pin::offset).min();
    }

    List<ReadShareGroupStateResponseData.PendingTransaction> read(SharePartitionKey key) {
        List<ReadShareGroupStateResponseData.PendingTransaction> result = new ArrayList<>();
        pending.forEach((k, intent) -> {
            if (k.sharePartition().equals(key)) {
                result.add(new ReadShareGroupStateResponseData.PendingTransaction()
                    .setTransactionalId(intent.transactionalId())
                    .setProducerId(k.owner().producerId())
                    .setProducerEpoch(k.owner().producerEpoch())
                    .setStateBatches(intent.batches().stream().map(batch -> new ReadShareGroupStateResponseData.StateBatch()
                        .setFirstOffset(batch.firstOffset()).setLastOffset(batch.lastOffset())
                        .setDeliveryState(batch.deliveryState()).setDeliveryCount(batch.deliveryCount())).toList()));
            }
        });
        result.sort(Comparator.comparingLong(ReadShareGroupStateResponseData.PendingTransaction::producerId)
            .thenComparingInt(ReadShareGroupStateResponseData.PendingTransaction::producerEpoch));
        return result;
    }

    Optional<ShareCoordinatorShard.TransactionCompletion> completion(Owner owner) {
        Set<SharePartitionKey> keys = pins.entrySet().stream()
            .filter(entry -> entry.getKey().owner().equals(owner) && entry.getValue().resolved())
            .map(entry -> entry.getKey().sharePartition()).collect(Collectors.toSet());
        if (keys.isEmpty()) return Optional.empty();
        String transactionalId = pins.entrySet().stream().filter(entry -> entry.getKey().owner().equals(owner) && entry.getValue().resolved())
            .map(entry -> entry.getValue().transactionalId()).findFirst().orElseThrow();
        return Optional.of(new ShareCoordinatorShard.TransactionCompletion(
            transactionalId, owner.producerId(), owner.producerEpoch(), Set.copyOf(keys)));
    }

    static boolean overlaps(PersisterStateBatch a, PersisterStateBatch b) {
        return a.firstOffset() <= b.lastOffset() && b.firstOffset() <= a.lastOffset();
    }
}
