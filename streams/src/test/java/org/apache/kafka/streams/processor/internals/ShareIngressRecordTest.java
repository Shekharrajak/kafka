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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.processor.TaskId;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShareIngressRecordTest {
    @Test
    void shouldPreserveOriginalRecordContextAndTargetTask() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add("trace", "trace-value".getBytes(UTF_8));
        final ConsumerRecord<byte[], byte[]> sourceRecord = new ConsumerRecord<>(
            "source-topic",
            3,
            19L,
            42L,
            TimestampType.CREATE_TIME,
            0,
            0,
            "key".getBytes(UTF_8),
            "value".getBytes(UTF_8),
            headers,
            Optional.of(7)
        );
        final TaskId targetTaskId = new TaskId(2, 3);

        final ShareIngressRecord ingressRecord = new ShareIngressRecord(targetTaskId, sourceRecord);
        final ProcessorRecordContext context = ingressRecord.recordContext();

        assertEquals(targetTaskId, ingressRecord.targetTaskId());
        assertEquals("source-topic", context.topic());
        assertEquals(3, context.partition());
        assertEquals(19L, context.offset());
        assertEquals(42L, context.timestamp());
        assertEquals(Optional.of(7), ingressRecord.leaderEpoch());
        assertArrayEquals("key".getBytes(UTF_8), context.sourceRawKey());
        assertArrayEquals("value".getBytes(UTF_8), context.sourceRawValue());
        assertEquals("trace", context.headers().lastHeader("trace").key());
        assertArrayEquals("trace-value".getBytes(UTF_8), context.headers().lastHeader("trace").value());
    }

    @Test
    void shouldRequireTargetTask() {
        final ConsumerRecord<byte[], byte[]> sourceRecord = new ConsumerRecord<>("source-topic", 0, 0L, null, null);

        assertThrows(NullPointerException.class, () -> new ShareIngressRecord(null, sourceRecord));
    }

    @Test
    void shouldDefensivelyCopySourceAndExposedBytesAndHeaders() {
        final byte[] key = "key".getBytes(UTF_8);
        final byte[] value = "value".getBytes(UTF_8);
        final byte[] headerValue = "header".getBytes(UTF_8);
        final RecordHeaders headers = new RecordHeaders();
        headers.add("header", headerValue);
        final ConsumerRecord<byte[], byte[]> sourceRecord = new ConsumerRecord<>(
            "source-topic",
            0,
            0L,
            0L,
            TimestampType.CREATE_TIME,
            0,
            0,
            key,
            value,
            headers,
            Optional.empty()
        );
        final ShareIngressRecord ingressRecord = new ShareIngressRecord(new TaskId(0, 0), sourceRecord);

        key[0] = 'K';
        value[0] = 'V';
        headerValue[0] = 'H';
        headers.add("later", "later-value".getBytes(UTF_8));

        assertArrayEquals("key".getBytes(UTF_8), ingressRecord.key());
        assertArrayEquals("value".getBytes(UTF_8), ingressRecord.value());
        assertArrayEquals("header".getBytes(UTF_8), ingressRecord.headers().lastHeader("header").value());
        assertEquals(null, ingressRecord.headers().lastHeader("later"));

        ingressRecord.key()[0] = 'K';
        ingressRecord.value()[0] = 'V';
        ingressRecord.headers().lastHeader("header").value()[0] = 'H';

        assertArrayEquals("key".getBytes(UTF_8), ingressRecord.key());
        assertArrayEquals("value".getBytes(UTF_8), ingressRecord.value());
        assertArrayEquals("header".getBytes(UTF_8), ingressRecord.headers().lastHeader("header").value());
    }
}
