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

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShareIngressAssignmentTest {
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
}
