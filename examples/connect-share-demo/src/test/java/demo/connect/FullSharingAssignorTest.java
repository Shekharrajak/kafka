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
package demo.connect;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FullSharingAssignorTest {
    @Test
    void allNineMembersCanAcquireEveryInitializedSubscribedPartition() {
        Uuid topic = Uuid.randomUuid();
        Uuid other = Uuid.randomUuid();
        GroupSpec group = mock(GroupSpec.class);
        MemberSubscription subscription = mock(MemberSubscription.class);
        SubscribedTopicDescriber topics = mock(SubscribedTopicDescriber.class);
        var members = IntStream.range(0, 9).mapToObj(Integer::toString).toList();
        when(group.memberIds()).thenReturn(members);
        when(subscription.subscribedTopicIds()).thenReturn(Set.of(topic));
        members.forEach(member -> when(group.memberSubscription(member)).thenReturn(subscription));
        when(topics.numPartitions(topic)).thenReturn(3);
        when(group.isPartitionAssignable(topic, 0)).thenReturn(true);
        when(group.isPartitionAssignable(topic, 2)).thenReturn(true);

        var assignment = new FullSharingAssignor().assign(group, topics);
        assertEquals(9, assignment.members().size());
        assignment.members().values().forEach(member -> {
            assertEquals(Set.of(topic), member.partitions().keySet());
            assertEquals(Set.of(0, 2), member.partitions().get(topic));
            assertThrows(UnsupportedOperationException.class, () -> member.partitions().put(other, Set.of(0)));
        });
    }

    @Test
    void missingTopicMetadataDoesNotCreateAnEmptySuccessfulAssignment() {
        Uuid topic = Uuid.randomUuid();
        GroupSpec group = mock(GroupSpec.class);
        MemberSubscription subscription = mock(MemberSubscription.class);
        SubscribedTopicDescriber topics = mock(SubscribedTopicDescriber.class);
        when(group.memberIds()).thenReturn(Set.of("one"));
        when(group.memberSubscription("one")).thenReturn(subscription);
        when(subscription.subscribedTopicIds()).thenReturn(Set.of(topic));
        when(topics.numPartitions(topic)).thenReturn(-1);
        assertThrows(PartitionAssignorException.class, () -> new FullSharingAssignor().assign(group, topics));
    }
}
