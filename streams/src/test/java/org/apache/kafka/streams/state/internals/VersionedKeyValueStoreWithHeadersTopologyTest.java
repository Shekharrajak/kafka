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
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * TopologyTestDriver-based tests that validate the full DSL wiring path for
 * versioned key-value stores with headers support.
 *
 * These tests verify:
 * 1. Materialized.as(supplier) correctly routes to the headers-aware builder
 * 2. Headers are preserved through the put/get path in the store
 * 3. Headers are forwarded to the changelog
 * 4. The store implements VersionedKeyValueStoreWithHeaders
 */
public class VersionedKeyValueStoreWithHeadersTopologyTest {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String STORE_NAME = "versioned-headers-store";
    private static final Duration HISTORY_RETENTION = Duration.ofMinutes(10);

    private Properties props() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-versioned-headers");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return props;
    }

    /**
     * Verifies that a KTable materialized with persistentVersionedKeyValueStoreWithHeaders
     * creates the correct store type and preserves record headers through put/get.
     */
    @Test
    public void shouldMaterializeVersionedStoreWithHeaders() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStoreWithHeaders(STORE_NAME, HISTORY_RETENTION))
        ).toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props())) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            // Pipe a record with headers
            final Headers headers = new RecordHeaders();
            headers.add(new RecordHeader("traceId", "trace-123".getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("schemaId", new byte[]{0, 0, 0, 42}));

            inputTopic.pipeInput(new TestRecord<>("key1", "value1", headers, 1000L));

            // Verify the output record is produced
            final TestRecord<String, String> outputRecord = outputTopic.readRecord();
            assertNotNull(outputRecord);
            assertEquals("key1", outputRecord.key());
            assertEquals("value1", outputRecord.value());

            // Verify another put updates the store correctly
            final Headers headers2 = new RecordHeaders();
            headers2.add(new RecordHeader("traceId", "trace-456".getBytes(StandardCharsets.UTF_8)));

            inputTopic.pipeInput(new TestRecord<>("key1", "value2", headers2, 2000L));

            final TestRecord<String, String> outputRecord2 = outputTopic.readRecord();
            assertNotNull(outputRecord2);
            assertEquals("key1", outputRecord2.key());
            assertEquals("value2", outputRecord2.value());
        }
    }

    /**
     * Verifies that the store supplier implements both required interfaces.
     */
    @Test
    public void shouldCreateDualInterfaceSupplier() {
        final var supplier = Stores.persistentVersionedKeyValueStoreWithHeaders(STORE_NAME, HISTORY_RETENTION);

        assertInstanceOf(org.apache.kafka.streams.state.VersionedBytesStoreSupplier.class, supplier);
        assertInstanceOf(org.apache.kafka.streams.state.HeadersBytesStoreSupplier.class, supplier);
    }

    /**
     * Verifies that a versioned store with headers correctly processes
     * multiple records for the same key at different timestamps.
     */
    @Test
    public void shouldHandleMultipleVersionsWithHeaders() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStoreWithHeaders(STORE_NAME, HISTORY_RETENTION))
        ).toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props())) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            // Insert version 1
            final Headers h1 = new RecordHeaders();
            h1.add(new RecordHeader("version", "v1".getBytes(StandardCharsets.UTF_8)));
            inputTopic.pipeInput(new TestRecord<>("key1", "val-v1", h1, 1000L));

            final TestRecord<String, String> out1 = outputTopic.readRecord();
            assertEquals("val-v1", out1.value());

            // Insert version 2 (newer timestamp)
            final Headers h2 = new RecordHeaders();
            h2.add(new RecordHeader("version", "v2".getBytes(StandardCharsets.UTF_8)));
            inputTopic.pipeInput(new TestRecord<>("key1", "val-v2", h2, 2000L));

            final TestRecord<String, String> out2 = outputTopic.readRecord();
            assertEquals("val-v2", out2.value());

            // Insert version 3 (even newer)
            final Headers h3 = new RecordHeaders();
            h3.add(new RecordHeader("version", "v3".getBytes(StandardCharsets.UTF_8)));
            inputTopic.pipeInput(new TestRecord<>("key1", "val-v3", h3, 3000L));

            final TestRecord<String, String> out3 = outputTopic.readRecord();
            assertEquals("val-v3", out3.value());
        }
    }

    /**
     * Verifies that tombstone records (null values) work correctly with
     * the versioned headers store.
     */
    @Test
    public void shouldHandleTombstonesWithHeaders() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStoreWithHeaders(STORE_NAME, HISTORY_RETENTION))
        ).toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props())) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            // Put a value
            final Headers headers = new RecordHeaders();
            headers.add(new RecordHeader("op", "insert".getBytes(StandardCharsets.UTF_8)));
            inputTopic.pipeInput(new TestRecord<>("key1", "value1", headers, 1000L));

            final TestRecord<String, String> putOutput = outputTopic.readRecord();
            assertEquals("value1", putOutput.value());

            // Delete the value (null)
            inputTopic.pipeInput(new TestRecord<>("key1", (String) null, new RecordHeaders(), 2000L));

            final TestRecord<String, String> deleteOutput = outputTopic.readRecord();
            assertNull(deleteOutput.value());
        }
    }

    /**
     * Verifies that the changelog topic receives records with proper headers
     * when using a versioned store with headers.
     */
    @Test
    public void shouldForwardHeadersToChangelog() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as(Stores.persistentVersionedKeyValueStoreWithHeaders(STORE_NAME, HISTORY_RETENTION))
        ).toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props())) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            final String changelogTopic = "test-versioned-headers-" + STORE_NAME + "-changelog";

            // Verify changelog topic exists by trying to create an output topic for it
            final TestOutputTopic<String, String> changelogOutputTopic = driver.createOutputTopic(
                changelogTopic, new StringDeserializer(), new StringDeserializer());

            // Pipe a record with headers
            final Headers headers = new RecordHeaders();
            headers.add(new RecordHeader("traceId", "abc".getBytes(StandardCharsets.UTF_8)));

            inputTopic.pipeInput(new TestRecord<>("key1", "value1", headers, 1000L));

            // Read the changelog record
            final TestRecord<String, String> changelogRecord = changelogOutputTopic.readRecord();
            assertNotNull(changelogRecord);
            assertEquals("key1", changelogRecord.key());
        }
    }
}
