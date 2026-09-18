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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.processor.TaskId;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShareIngressDeliveryTest {
    @Test
    void shouldPreserveIngressProgressAndOriginalProcessorRecordContext() {
        final TaskId taskId = new TaskId(0, 1);
        final ShareIngressAssignment assignment = new ShareIngressAssignment(
            "application",
            Map.of(taskId, Set.of(new TopicPartition("source", 1)))
        );
        final ShareIngressRecord sourceRecord = new ShareIngressRecord(
            taskId,
            new ConsumerRecord<>(
                "source",
                1,
                42L,
                100L,
                TimestampType.CREATE_TIME,
                3,
                5,
                "key".getBytes(UTF_8),
                "value".getBytes(UTF_8),
                new RecordHeaders(),
                Optional.of(4)
            )
        );
        final ConsumerRecord<byte[], byte[]> ingressRecord = new ConsumerRecord<>(
            "application-source-share-ingress",
            1,
            9L,
            200L,
            TimestampType.CREATE_TIME,
            -1,
            0,
            null,
            ShareIngressRecordSerde.serialize(sourceRecord),
            new RecordHeaders(),
            Optional.of(8)
        );

        final ShareIngressDelivery delivery = ShareIngressDelivery.decode(taskId, ingressRecord, assignment);

        assertEquals(new TopicPartition("application-source-share-ingress", 1), delivery.ingressPartition());
        assertEquals(9L, delivery.ingressOffset());
        assertEquals(Optional.of(8), delivery.ingressLeaderEpoch());
        assertEquals("source", delivery.sourceRecord().topic());
        assertEquals(1, delivery.sourceRecord().partition());
        assertEquals(42L, delivery.sourceRecord().offset());
        assertEquals(100L, delivery.sourceRecord().timestamp());
        assertArrayEquals("key".getBytes(UTF_8), delivery.sourceRecord().key());
        assertArrayEquals("value".getBytes(UTF_8), delivery.sourceRecord().value());
    }

    @Test
    void shouldRejectIngressRecordForTheWrongTaskOrSourcePartition() {
        final TaskId taskId = new TaskId(0, 0);
        final ShareIngressAssignment assignment = new ShareIngressAssignment(
            "application",
            Map.of(
                taskId, Set.of(new TopicPartition("source-a", 0)),
                new TaskId(0, 1), Set.of(new TopicPartition("source-b", 0))
            )
        );
        final ConsumerRecord<byte[], byte[]> ingressRecord = new ConsumerRecord<>(
            "application-source-a-share-ingress",
            0,
            1L,
            null,
            ShareIngressRecordSerde.serialize(new ShareIngressRecord(
                taskId,
                new ConsumerRecord<>("source-a", 0, 3L, null, null)
            ))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> ShareIngressDelivery.decode(new TaskId(0, 1), ingressRecord, assignment)
        );

        final ConsumerRecord<byte[], byte[]> mismatchedIngressRecord = new ConsumerRecord<>(
            "application-source-a-share-ingress",
            0,
            1L,
            null,
            ShareIngressRecordSerde.serialize(new ShareIngressRecord(
                taskId,
                new ConsumerRecord<>("source-b", 0, 3L, null, null)
            ))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> ShareIngressDelivery.decode(taskId, mismatchedIngressRecord, assignment)
        );
    }
}
