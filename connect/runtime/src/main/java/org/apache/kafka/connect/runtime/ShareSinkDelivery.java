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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

class ShareSinkDelivery {
    private final List<ConsumerRecord<byte[], byte[]>> originals = new ArrayList<>();
    private final List<SinkRecord> converted = new ArrayList<>();

    ShareSinkDelivery(ConsumerRecords<byte[], byte[]> records,
                      Function<ConsumerRecord<byte[], byte[]>, SinkRecord> converter) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            originals.add(record);
            SinkRecord result = converter.apply(record);
            if (result != null) {
                converted.add(result);
            }
        }
    }

    void complete(ShareSinkTask task, ShareConsumer<byte[], byte[]> consumer, Duration timeout) {
        if (originals.isEmpty()) {
            return;
        }
        if (!converted.isEmpty()) {
            task.put(List.copyOf(converted));
        }
        for (ConsumerRecord<byte[], byte[]> record : originals) {
            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
        }
        for (Optional<KafkaException> failure : consumer.commitSync(timeout).values()) {
            if (failure.isPresent()) {
                throw failure.get();
            }
        }
    }
}
