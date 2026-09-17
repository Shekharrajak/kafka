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
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.processor.TaskId;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShareIngressRecordSerdeTest {
    @Test
    void shouldRoundTripOriginalRecordContextWithTaskAssignedAtConsumption() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("trace", "trace-value".getBytes(UTF_8));
        headers.add("empty", new byte[0]);
        headers.add("null", null);
        final ShareIngressRecord source = new ShareIngressRecord(
            new TaskId(2, 3),
            new ConsumerRecord<>(
                "source-topic",
                3,
                19L,
                42L,
                TimestampType.CREATE_TIME,
                0,
                0,
                null,
                "value".getBytes(UTF_8),
                headers,
                Optional.of(7)
            )
        );
        final TaskId consumingTask = new TaskId(5, 3, "named-topology");

        final ShareIngressRecord restored = ShareIngressRecordSerde.deserialize(
            consumingTask,
            ShareIngressRecordSerde.serialize(source)
        );

        assertEquals(consumingTask, restored.targetTaskId());
        assertEquals("source-topic", restored.topic());
        assertEquals(3, restored.partition());
        assertEquals(19L, restored.offset());
        assertEquals(42L, restored.timestamp());
        assertEquals(Optional.of(7), restored.leaderEpoch());
        assertEquals(null, restored.key());
        assertArrayEquals("value".getBytes(UTF_8), restored.value());
        final Header[] restoredHeaders = restored.headers().toArray();
        assertEquals("trace", restoredHeaders[0].key());
        assertArrayEquals("trace-value".getBytes(UTF_8), restoredHeaders[0].value());
        assertEquals("empty", restoredHeaders[1].key());
        assertArrayEquals(new byte[0], restoredHeaders[1].value());
        assertEquals("null", restoredHeaders[2].key());
        assertEquals(null, restoredHeaders[2].value());
    }

    @Test
    void shouldRejectUnknownOrTruncatedIngressRecords() {
        assertThrows(SerializationException.class, () -> ShareIngressRecordSerde.deserialize(new TaskId(0, 0), new byte[] {1}));
        assertThrows(SerializationException.class, () -> ShareIngressRecordSerde.deserialize(new TaskId(0, 0), new byte[] {0}));
    }
}
