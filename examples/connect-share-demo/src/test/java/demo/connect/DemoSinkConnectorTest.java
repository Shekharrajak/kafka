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
package demo.connect;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkConnector;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DemoSinkConnectorTest {
    @Test
    void bothConnectorsAreDiscoverableUsingTheSinkServiceContract() {
        Set<Class<?>> discovered = ServiceLoader.load(SinkConnector.class).stream()
                .map(ServiceLoader.Provider::type).collect(Collectors.toSet());
        assertEquals(Set.of(DemoSinkConnector.class, ShareDemoSinkConnector.class), discovered);
    }

    @Test
    void emitsNineIndependentTaskConfigurationsForEachMode() {
        for (String mode : List.of("classic", "share")) {
            DemoSinkConnector connector = "share".equals(mode) ? new ShareDemoSinkConnector() : new DemoSinkConnector();
            assertEquals("share".equals(mode) ? ShareDemoSinkTask.class : ClassicDemoSinkTask.class, connector.taskClass());
            connector.start(config(mode));
            assertEquals("share".equals(mode) ? ShareDemoSinkTask.class : ClassicDemoSinkTask.class, connector.taskClass());
            List<Map<String, String>> tasks = connector.taskConfigs(9);
            assertEquals(9, tasks.size());
            assertEquals(IntStream.range(0, 9).mapToObj(Integer::toString).toList(),
                    tasks.stream().map(task -> task.get(DemoSinkConnector.TASK_ID)).toList());
            tasks.get(0).put(DemoSinkConnector.OUTPUT_TOPIC, "changed");
            assertEquals("outputs", tasks.get(1).get(DemoSinkConnector.OUTPUT_TOPIC));
            assertEquals("outputs", connector.taskConfigs(1).get(0).get(DemoSinkConnector.OUTPUT_TOPIC));
        }
    }

    @Test
    void rejectsUnsupportedModeAndMissingDestination() {
        DemoSinkConnector connector = new DemoSinkConnector();
        assertThrows(ConfigException.class, () -> connector.start(config("unknown")));
        assertThrows(ConfigException.class, () -> connector.start(config("share")));
        assertThrows(ConfigException.class, () -> new ShareDemoSinkConnector().start(config("classic")));
        Map<String, String> missingOutput = new HashMap<>(config("share"));
        missingOutput.remove(DemoSinkConnector.OUTPUT_TOPIC);
        assertThrows(ConfigException.class, () -> connector.start(missingOutput));
    }

    private static Map<String, String> config(String mode) {
        return Map.of(DemoSinkConnector.MODE, mode,
                DemoSinkConnector.BOOTSTRAP_SERVERS, "broker:9092",
                DemoSinkConnector.OUTPUT_TOPIC, "outputs");
    }
}
