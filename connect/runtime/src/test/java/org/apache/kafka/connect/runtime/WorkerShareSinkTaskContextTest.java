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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerShareSinkTaskContextTest {
    private final TopicPartition partition = new TopicPartition("events", 0);
    private final Set<TopicPartition> observed = new HashSet<>();
    private final WorkerShareSinkTaskContext context = new WorkerShareSinkTaskContext(() -> Map.of("key", "value"), observed, null);

    @Test
    void returnsObservedPartitionsWithoutClaimingExclusiveOwnership() {
        observed.add(partition);
        assertEquals(Set.of(partition), context.assignment());
        assertThrows(UnsupportedOperationException.class, () -> context.assignment().clear());
        assertEquals("value", context.configs().get("key"));
    }

    @Test
    void rejectsOffsetSeekingAndPartitionPause() {
        assertThrows(UnsupportedOperationException.class, () -> context.offset(partition, 100));
        assertThrows(UnsupportedOperationException.class, () -> context.offset(Map.of(partition, 100L)));
        assertThrows(UnsupportedOperationException.class, () -> context.pause(partition));
        assertThrows(UnsupportedOperationException.class, () -> context.resume(partition));
    }
}
