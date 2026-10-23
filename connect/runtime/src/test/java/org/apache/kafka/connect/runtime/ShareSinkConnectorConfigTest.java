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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShareSinkConnectorConfigTest {
    @Test
    void defaultsToConsumerGroups() {
        Map<String, String> props = Map.of("topics", "events");
        SinkConnectorConfig.validate(props);
        assertFalse(SinkConnectorConfig.usesShareConsumer(props));
    }

    @Test
    void acceptsExplicitShareConsumption() {
        Map<String, String> props = Map.of("topics", "events", "consumer.type", "share");
        SinkConnectorConfig.validate(props);
        assertTrue(SinkConnectorConfig.usesShareConsumer(props));
    }

    @Test
    void rejectsUnsupportedShareConfigurations() {
        for (Map.Entry<String, String> setting : Map.of(
                "topics.regex", ".*", "errors.tolerance", "all", "errors.deadletterqueue.topic.name", "dlq").entrySet()) {
            Map<String, String> props = new HashMap<>(Map.of("topics", "events", "consumer.type", "share"));
            props.put(setting.getKey(), setting.getValue());
            assertThrows(ConfigException.class, () -> SinkConnectorConfig.validate(props));
        }
    }

    @Test
    void rejectsUnknownConsumerType() {
        assertThrows(ConfigException.class, () -> SinkConnectorConfig.validate(Map.of("topics", "events", "consumer.type", "other")));
    }
}
