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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class FullSharingAssignor implements ShareGroupPartitionAssignor {
    @Override
    public String name() {
        return "demo-full-sharing";
    }

    @Override
    public GroupAssignment assign(GroupSpec group, SubscribedTopicDescriber topics) {
        Map<String, MemberAssignment> assignments = new HashMap<>();
        for (String member : group.memberIds()) {
            Map<Uuid, Set<Integer>> assignment = new HashMap<>();
            for (Uuid topic : group.memberSubscription(member).subscribedTopicIds()) {
                int count = topics.numPartitions(topic);
                if (count < 0) {
                    throw new PartitionAssignorException("Missing topic metadata for " + topic);
                }
                Set<Integer> partitions = new HashSet<>();
                for (int partition = 0; partition < count; partition++) {
                    if (group.isPartitionAssignable(topic, partition)) {
                        partitions.add(partition);
                    }
                }
                if (!partitions.isEmpty()) {
                    assignment.put(topic, Set.copyOf(partitions));
                }
            }
            assignments.put(member, new Assignment(Map.copyOf(assignment)));
        }
        return new GroupAssignment(Map.copyOf(assignments));
    }

    private record Assignment(Map<Uuid, Set<Integer>> partitions) implements MemberAssignment { }
}
