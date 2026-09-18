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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statelessTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShareIngressAssignmentTest {
    @Test
    void shouldDeriveIngressOwnershipFromActiveTaskInputs() {
        final TaskId firstTaskId = new TaskId(0, 0);
        final TaskId secondTaskId = new TaskId(0, 1);
        final StreamTask firstTask = statelessTask(firstTaskId)
            .withInputPartitions(Set.of(new TopicPartition("share-source", 0), new TopicPartition("regular-source", 0)))
            .build();
        final StreamTask secondTask = statelessTask(secondTaskId)
            .withInputPartitions(Set.of(new TopicPartition("share-source", 1)))
            .build();

        final ShareIngressAssignment assignment = ShareIngressAssignment.fromActiveTasks(
            "application",
            List.of(firstTask, secondTask),
            Set.of("share-source")
        );

        assertEquals(firstTaskId, assignment.targetTask(new TopicPartition("share-source", 0)));
        assertEquals(secondTaskId, assignment.targetTask(new TopicPartition("share-source", 1)));
        assertThrows(IllegalArgumentException.class,
            () -> assignment.targetTask(new TopicPartition("regular-source", 0)));
    }
    @Test
    void shouldRouteEachSourcePartitionToItsOwningTaskAndIngressPartition() {
        final ShareIngressAssignment assignment = new ShareIngressAssignment(
            "application",
            Map.of(
                new TaskId(0, 0), Set.of(new TopicPartition("source-a", 0), new TopicPartition("source-b", 0)),
                new TaskId(0, 1), Set.of(new TopicPartition("source-a", 1), new TopicPartition("source-b", 1))
            )
        );

        assertEquals(new TaskId(0, 1), assignment.targetTask(new TopicPartition("source-b", 1)));
        assertEquals(
            new TopicPartition("application-source-b-share-ingress", 1),
            assignment.ingressPartition(new TopicPartition("source-b", 1))
        );
    }

    @Test
    void shouldRejectUnownedAndAmbiguousSourcePartitions() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ShareIngressAssignment(
                "application",
                Map.of(
                    new TaskId(0, 0), Set.of(new TopicPartition("source", 0)),
                    new TaskId(1, 0), Set.of(new TopicPartition("source", 0))
                )
            )
        );

        final ShareIngressAssignment assignment = new ShareIngressAssignment(
            "application",
            Map.of(new TaskId(0, 0), Set.of(new TopicPartition("source", 0)))
        );

        assertThrows(IllegalArgumentException.class, () -> assignment.targetTask(new TopicPartition("source", 1)));
    }

    @Test
    void shouldDeriveIngressPartitionsForTheCurrentTaskAssignment() {
        final TaskId firstTask = new TaskId(0, 0);
        final TaskId secondTask = new TaskId(0, 1);
        final ShareIngressAssignment assignment = new ShareIngressAssignment(
            "application",
            Map.of(
                firstTask, Set.of(new TopicPartition("source-a", 0), new TopicPartition("source-b", 0)),
                secondTask, Set.of(new TopicPartition("source-a", 1))
            )
        );

        assertEquals(
            Map.of(
                firstTask, Set.of(
                    new TopicPartition("application-source-a-share-ingress", 0),
                    new TopicPartition("application-source-b-share-ingress", 0)
                ),
                secondTask, Set.of(new TopicPartition("application-source-a-share-ingress", 1))
            ),
            assignment.ingressPartitionsByTask()
        );
        assertEquals(firstTask, assignment.targetTaskForIngress(new TopicPartition("application-source-b-share-ingress", 0)));
        assertEquals(
            new TopicPartition("source-b", 0),
            assignment.sourcePartitionForIngress(new TopicPartition("application-source-b-share-ingress", 0))
        );
    }
}
