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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

final class ShareIngressRecord {
    private final TaskId targetTaskId;
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;
    private final Headers headers;
    private final Optional<Integer> leaderEpoch;

    ShareIngressRecord(final TaskId targetTaskId, final ConsumerRecord<byte[], byte[]> sourceRecord) {
        this.targetTaskId = Objects.requireNonNull(targetTaskId, "targetTaskId cannot be null");
        Objects.requireNonNull(sourceRecord, "sourceRecord cannot be null");
        this.topic = sourceRecord.topic();
        this.partition = sourceRecord.partition();
        this.offset = sourceRecord.offset();
        this.timestamp = sourceRecord.timestamp();
        this.key = copyBytes(sourceRecord.key());
        this.value = copyBytes(sourceRecord.value());
        this.headers = copyHeaders(sourceRecord.headers());
        this.leaderEpoch = sourceRecord.leaderEpoch();
    }

    TaskId targetTaskId() {
        return targetTaskId;
    }

    String topic() {
        return topic;
    }

    int partition() {
        return partition;
    }

    long offset() {
        return offset;
    }

    long timestamp() {
        return timestamp;
    }

    byte[] key() {
        return copyBytes(key);
    }

    byte[] value() {
        return copyBytes(value);
    }

    Headers headers() {
        return copyHeaders(headers);
    }

    Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    ProcessorRecordContext recordContext() {
        return new ProcessorRecordContext(timestamp, offset, partition, topic, headers(), key(), value());
    }

    private static byte[] copyBytes(final byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    private static Headers copyHeaders(final Headers sourceHeaders) {
        final RecordHeaders copiedHeaders = new RecordHeaders();
        for (final Header header : sourceHeaders) {
            copiedHeaders.add(header.key(), copyBytes(header.value()));
        }
        return copiedHeaders;
    }
}
