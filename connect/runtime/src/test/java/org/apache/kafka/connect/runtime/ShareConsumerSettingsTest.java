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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.errors.ConnectException;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShareConsumerSettingsTest {
    @Test
    void removesConsumerDefaultsAndPreservesSecurity() {
        Map<String, Object> base = Map.of("enable.auto.commit", "false", "auto.offset.reset", "earliest",
                "security.protocol", "SASL_SSL", "group.id", "connect-test");
        Map<String, Object> result = ShareConsumerSettings.create("test", base, Map.of(), Map.of());
        assertFalse(result.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertFalse(result.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("SASL_SSL", result.get("security.protocol"));
        assertEquals("explicit", result.get("share.acknowledgement.mode"));
        assertEquals("connect-share-test", result.get("group.id"));
        assertEquals("earliest", base.get("auto.offset.reset"));
    }

    @Test
    void rejectsExplicitOffsetOverrides() {
        for (String key : java.util.List.of("enable.auto.commit", "auto.offset.reset")) {
            assertThrows(ConnectException.class, () -> ShareConsumerSettings.create("test", Map.of(), Map.of(key, "value"), Map.of()));
            assertThrows(ConnectException.class, () -> ShareConsumerSettings.create("test", Map.of(), Map.of(), Map.of(key, "value")));
        }
    }

    @Test
    void preservesExplicitGroupId() {
        Map<String, Object> result = ShareConsumerSettings.create("test", Map.of("group.id", "custom"), Map.of(), Map.of("group.id", "custom"));
        assertEquals("custom", result.get("group.id"));
    }

    @Test
    void rejectsImplicitAcknowledgements() {
        assertThrows(ConnectException.class, () -> ShareConsumerSettings.create("test", Map.of("share.acknowledgement.mode", "implicit"), Map.of(), Map.of()));
    }
}
