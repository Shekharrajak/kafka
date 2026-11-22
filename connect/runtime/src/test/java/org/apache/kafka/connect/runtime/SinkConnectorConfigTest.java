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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfigTest.MOCK_PLUGINS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SinkConnectorConfigTest {
    @Test
    public void testClassicDefaultIsUnchanged() {
        Map<String, String> props = baseProps();
        SinkConnectorConfig config = new SinkConnectorConfig(MOCK_PLUGINS, props);
        assertEquals("classic", config.getString(SinkConnectorConfig.CONSUMER_TYPE_CONFIG));
        assertFalse(SinkConnectorConfig.isTransactionalShareSink(props));
        assertDoesNotThrow(() -> SinkConnectorConfig.validate(props));
    }

    @Test
    public void testExplicitTransactionalShareOptIn() {
        Map<String, String> props = shareProps();
        assertTrue(SinkConnectorConfig.isTransactionalShareSink(props));
        assertDoesNotThrow(() -> new SinkConnectorConfig(MOCK_PLUGINS, props));
        assertDoesNotThrow(() -> SinkConnectorConfig.validate(props));
    }

    @ParameterizedTest
    @CsvSource({
        "share.group.id,''",
        "share.group.id,connect-sink",
        "share.delivery.guarantee,at_least_once",
        "consumer.type,classic",
        "topics.regex,input-.*",
        "errors.tolerance,all",
        "errors.deadletterqueue.topic.name,failed-records"
    })
    public void testRejectsUnsupportedShareRecoveryModes(String key, String value) {
        Map<String, String> props = shareProps();
        props.put(key, value);
        assertThrows(ConfigException.class, () -> SinkConnectorConfig.validate(props));
        assertThrows(ConfigException.class, () -> new SinkConnectorConfig(MOCK_PLUGINS, props));
    }

    @Test
    public void testShareRequiresExplicitTopicsAndDedicatedGroup() {
        Map<String, String> missingTopics = shareProps();
        missingTopics.remove(SinkConnectorConfig.TOPICS_CONFIG);
        assertThrows(ConfigException.class, () -> SinkConnectorConfig.validate(missingTopics));
        Map<String, String> missingGroup = shareProps();
        missingGroup.remove(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG);
        assertThrows(ConfigException.class, () -> SinkConnectorConfig.validate(missingGroup));
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.NAME_CONFIG, "sink");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, ConnectorConfigTest.TestConnector.class.getName());
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "input");
        return props;
    }

    private Map<String, String> shareProps() {
        Map<String, String> props = baseProps();
        props.put(SinkConnectorConfig.CONSUMER_TYPE_CONFIG, "share");
        props.put(SinkConnectorConfig.SHARE_DELIVERY_GUARANTEE_CONFIG, "exactly_once");
        props.put(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG, "dedicated-input");
        return props;
    }
}
