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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.util.ConnectUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Function;

class SinkRecordConverter {
    private static final Logger log = LoggerFactory.getLogger(SinkRecordConverter.class);
    private final Plugin<Converter> keyConverterPlugin;
    private final Plugin<Converter> valueConverterPlugin;
    private final Plugin<HeaderConverter> headerConverterPlugin;
    private final RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator;
    private final TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformationChain;
    private final Function<ClassLoader, LoaderSwap> pluginLoaderSwapper;
    private final Consumer<String> activeTopic;

    SinkRecordConverter(Plugin<Converter> keyConverterPlugin,
                        Plugin<Converter> valueConverterPlugin,
                        Plugin<HeaderConverter> headerConverterPlugin,
                        RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator,
                        TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformationChain,
                        Function<ClassLoader, LoaderSwap> pluginLoaderSwapper,
                        Consumer<String> activeTopic) {
        this.keyConverterPlugin = keyConverterPlugin;
        this.valueConverterPlugin = valueConverterPlugin;
        this.headerConverterPlugin = headerConverterPlugin;
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.transformationChain = transformationChain;
        this.pluginLoaderSwapper = pluginLoaderSwapper;
        this.activeTopic = activeTopic;
    }

    SinkRecord convert(ProcessingContext<ConsumerRecord<byte[], byte[]>> context, final ConsumerRecord<byte[], byte[]> msg) {
        SchemaAndValue keyAndSchema = retryWithToleranceOperator.execute(context, () -> {
            try (LoaderSwap swap = pluginLoaderSwapper.apply(keyConverterPlugin.get().getClass().getClassLoader())) {
                return keyConverterPlugin.get().toConnectData(msg.topic(), msg.headers(), msg.key());
            }
        }, Stage.KEY_CONVERTER, keyConverterPlugin.get().getClass());

        SchemaAndValue valueAndSchema = retryWithToleranceOperator.execute(context, () -> {
            try (LoaderSwap swap = pluginLoaderSwapper.apply(valueConverterPlugin.get().getClass().getClassLoader())) {
                return valueConverterPlugin.get().toConnectData(msg.topic(), msg.headers(), msg.value());
            }
        }, Stage.VALUE_CONVERTER, valueConverterPlugin.get().getClass());

        Headers headers = retryWithToleranceOperator.execute(context, () -> convertHeadersFor(msg), Stage.HEADER_CONVERTER, headerConverterPlugin.get().getClass());

        if (context.failed()) {
            return null;
        }

        Long timestamp = ConnectUtils.checkAndConvertTimestamp(msg.timestamp());
        SinkRecord origRecord = new SinkRecord(msg.topic(), msg.partition(),
                keyAndSchema.schema(), keyAndSchema.value(),
                valueAndSchema.schema(), valueAndSchema.value(),
                msg.offset(),
                timestamp,
                msg.timestampType(),
                headers);
        log.trace("{} Applying transformations to record in topic '{}' partition {} at offset {} and timestamp {} with key {} and value {}",
                this, msg.topic(), msg.partition(), msg.offset(), timestamp, keyAndSchema.value(), valueAndSchema.value());
        activeTopic.accept(origRecord.topic());

        // Apply the transformations
        SinkRecord transformedRecord = transformationChain.apply(context, origRecord);
        if (transformedRecord == null) {
            return null;
        }
        // Error reporting will need to correlate each sink record with the original consumer record
        return new InternalSinkRecord(context, transformedRecord);
    }

    private Headers convertHeadersFor(ConsumerRecord<byte[], byte[]> record) {
        Headers result = new ConnectHeaders();
        org.apache.kafka.common.header.Headers recordHeaders = record.headers();
        if (recordHeaders != null) {
            String topic = record.topic();
            for (org.apache.kafka.common.header.Header recordHeader : recordHeaders) {
                try (LoaderSwap swap = pluginLoaderSwapper.apply(headerConverterPlugin.get().getClass().getClassLoader())) {
                    SchemaAndValue schemaAndValue = headerConverterPlugin.get().toConnectHeader(topic, recordHeader.key(), recordHeader.value());
                    result.add(recordHeader.key(), schemaAndValue);
                }
            }
        }
        return result;
    }

}
