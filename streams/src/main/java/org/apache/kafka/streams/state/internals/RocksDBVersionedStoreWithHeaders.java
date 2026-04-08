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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.VersionedRecord;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A persistent, versioned key-value store based on RocksDB that additionally
 * preserves record headers.
 * <p>
 * Headers are embedded into the value bytes using the format:
 * {@code [headersSize(varint)][headersBytes][rawValue]}
 * before delegating to the parent {@link RocksDBVersionedStore}. On reads,
 * headers are extracted from the stored value bytes.
 */
public class RocksDBVersionedStoreWithHeaders
        extends RocksDBVersionedStore
        implements VersionedKeyValueStoreWithHeaders<Bytes, byte[]> {

    RocksDBVersionedStoreWithHeaders(final String name,
                                     final String metricsScope,
                                     final long historyRetention,
                                     final long segmentInterval) {
        super(name, metricsScope, historyRetention, segmentInterval);
    }

    @Override
    public long put(final Bytes key, final byte[] value, final long timestamp, final Headers headers) {
        Objects.requireNonNull(headers, "headers cannot be null");
        if (value == null) {
            // tombstone: delegate directly, no headers to embed
            return super.put(key, null, timestamp);
        }
        
        // Check if headers are empty and use fast path
        if (!headers.iterator().hasNext()) {
            final byte[] encodedValue = HeadersBytesStore.convertToHeaderFormat(value);
            return super.put(key, encodedValue, timestamp);
        }
        
        // Use shared HeadersSerializer infrastructure
        final HeadersSerializer.PreSerializedHeaders prep = HeadersSerializer.prepareSerialization(headers);
        final int payloadSize = prep.requiredBufferSizeForHeaders + value.length;
        final ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(prep.requiredBufferSizeForHeaders) + payloadSize);
        ByteUtils.writeVarint(prep.requiredBufferSizeForHeaders, buffer);
        HeadersSerializer.serialize(prep, buffer);
        buffer.put(value);
        return super.put(key, buffer.array(), timestamp);
    }

    @Override
    public long put(final Bytes key, final byte[] value, final long timestamp) {
        // non-headers put: embed empty headers
        return put(key, value, timestamp, new RecordHeaders());
    }

    @Override
    public VersionedRecord<byte[]> get(final Bytes key) {
        final VersionedRecord<byte[]> record = super.get(key);
        return decodeRecord(record);
    }

    @Override
    public VersionedRecord<byte[]> get(final Bytes key, final long asOfTimestamp) {
        final VersionedRecord<byte[]> record = super.get(key, asOfTimestamp);
        return decodeRecord(record);
    }

    @Override
    public VersionedRecord<byte[]> delete(final Bytes key, final long timestamp) {
        final VersionedRecord<byte[]> record = super.delete(key, timestamp);
        return decodeRecord(record);
    }

    private static VersionedRecord<byte[]> decodeRecord(final VersionedRecord<byte[]> record) {
        if (record == null) {
            return null;
        }
        final byte[] encodedValue = record.value();
        final Headers headers = Utils.headers(encodedValue);
        final byte[] rawValue = extractRawValue(encodedValue);
        if (record.validTo().isPresent()) {
            return new VersionedRecord<>(rawValue, record.timestamp(), record.validTo().get(), headers);
        } else {
            return new VersionedRecord<>(rawValue, record.timestamp(), headers);
        }
    }

    /**
     * Extract raw value from encoded value bytes, stripping the headers prefix.
     * Format: [headersSize(varint)][headersBytes][rawValue]
     */
    private static byte[] extractRawValue(final byte[] encodedValue) {
        if (encodedValue == null) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(encodedValue);
        final int headersSize = ByteUtils.readVarint(buffer);
        buffer.position(buffer.position() + headersSize);
        final byte[] rawValue = new byte[buffer.remaining()];
        buffer.get(rawValue);
        return rawValue;
    }

}
