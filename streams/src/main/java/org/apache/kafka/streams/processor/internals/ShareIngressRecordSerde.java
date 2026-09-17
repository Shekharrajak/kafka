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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.processor.TaskId;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

final class ShareIngressRecordSerde {
    private static final byte VERSION = 0;

    private ShareIngressRecordSerde() {
    }

    static byte[] serialize(final ShareIngressRecord record) {
        Objects.requireNonNull(record, "record cannot be null");
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(VERSION);
            writeRequiredBytes(output, record.topic().getBytes(StandardCharsets.UTF_8));
            output.writeInt(record.partition());
            output.writeLong(record.offset());
            output.writeLong(record.timestamp());
            output.writeBoolean(record.leaderEpoch().isPresent());
            if (record.leaderEpoch().isPresent()) {
                output.writeInt(record.leaderEpoch().get());
            }
            writeHeaders(output, record.headers());
            writeNullableBytes(output, record.key());
            writeNullableBytes(output, record.value());
            output.flush();
            return bytes.toByteArray();
        } catch (final IOException e) {
            throw new SerializationException("Unable to serialize share ingress record", e);
        }
    }

    static ShareIngressRecord deserialize(final TaskId targetTaskId, final byte[] serialized) {
        Objects.requireNonNull(targetTaskId, "targetTaskId cannot be null");
        Objects.requireNonNull(serialized, "serialized cannot be null");
        try {
            final ByteBuffer input = ByteBuffer.wrap(serialized);
            if (input.get() != VERSION) {
                throw new SerializationException("Unknown share ingress record version");
            }
            final String topic = new String(readRequiredBytes(input), StandardCharsets.UTF_8);
            final int partition = input.getInt();
            final long offset = input.getLong();
            final long timestamp = input.getLong();
            final byte leaderEpochPresent = input.get();
            final Optional<Integer> leaderEpoch;
            if (leaderEpochPresent == 0) {
                leaderEpoch = Optional.empty();
            } else if (leaderEpochPresent == 1) {
                leaderEpoch = Optional.of(input.getInt());
            } else {
                throw new SerializationException("Invalid share ingress leader epoch marker");
            }
            final Headers headers = readHeaders(input);
            final byte[] key = readNullableBytes(input);
            final byte[] value = readNullableBytes(input);
            if (input.hasRemaining()) {
                throw new SerializationException("Unexpected trailing bytes in share ingress record");
            }
            return new ShareIngressRecord(
                targetTaskId,
                new ConsumerRecord<>(
                    topic,
                    partition,
                    offset,
                    timestamp,
                    TimestampType.CREATE_TIME,
                    key == null ? -1 : key.length,
                    value == null ? -1 : value.length,
                    key,
                    value,
                    headers,
                    leaderEpoch
                )
            );
        } catch (final BufferUnderflowException | IllegalArgumentException e) {
            throw new SerializationException("Invalid share ingress record", e);
        }
    }

    private static void writeHeaders(final DataOutputStream output, final Headers headers) throws IOException {
        final Header[] allHeaders = headers.toArray();
        output.writeInt(allHeaders.length);
        for (final Header header : allHeaders) {
            writeRequiredBytes(output, header.key().getBytes(StandardCharsets.UTF_8));
            writeNullableBytes(output, header.value());
        }
    }

    private static Headers readHeaders(final ByteBuffer input) {
        final int count = input.getInt();
        if (count < 0 || count > input.remaining() / (2 * Integer.BYTES)) {
            throw new SerializationException("Invalid share ingress header count");
        }
        final RecordHeaders headers = new RecordHeaders();
        for (int index = 0; index < count; index++) {
            headers.add(new String(readRequiredBytes(input), StandardCharsets.UTF_8), readNullableBytes(input));
        }
        return headers;
    }

    private static void writeRequiredBytes(final DataOutputStream output, final byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private static void writeNullableBytes(final DataOutputStream output, final byte[] value) throws IOException {
        output.writeInt(value == null ? -1 : value.length);
        if (value != null) {
            output.write(value);
        }
    }

    private static byte[] readRequiredBytes(final ByteBuffer input) {
        final byte[] value = readNullableBytes(input);
        if (value == null) {
            throw new SerializationException("Unexpected null share ingress field");
        }
        return value;
    }

    private static byte[] readNullableBytes(final ByteBuffer input) {
        final int length = input.getInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > input.remaining()) {
            throw new SerializationException("Invalid share ingress field length");
        }
        final byte[] value = new byte[length];
        input.get(value);
        return value;
    }
}
