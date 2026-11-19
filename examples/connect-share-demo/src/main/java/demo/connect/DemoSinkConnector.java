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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DemoSinkConnector extends SinkConnector {
    static final String MODE = "demo.mode";
    static final String BOOTSTRAP_SERVERS = "demo.bootstrap.servers";
    static final String OUTPUT_TOPIC = "demo.output.topic";
    static final String TASK_ID = "demo.task.id";

    private static final ConfigDef CONFIG = new ConfigDef()
            .define(MODE, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.ValidString.in("classic", "share"),
                    ConfigDef.Importance.HIGH, "Consumer mode for the comparison.")
            .define(BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "Destination Kafka bootstrap servers.")
            .define(OUTPUT_TOPIC, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "Audit topic receiving every output occurrence.");

    private Map<String, String> properties;

    @Override
    public String version() {
        return "1";
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(CONFIG, props);
        String expectedMode = ShareSinkTask.class.isAssignableFrom(taskClass()) ? "share" : "classic";
        if (!expectedMode.equals(config.getString(MODE))) {
            throw new ConfigException(MODE, config.getString(MODE), "This connector requires " + expectedMode);
        }
        properties = Map.copyOf(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ClassicDemoSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> tasks = new ArrayList<>(maxTasks);
        for (int index = 0; index < maxTasks; index++) {
            Map<String, String> task = new HashMap<>(properties);
            task.put(TASK_ID, Integer.toString(index));
            tasks.add(task);
        }
        return tasks;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }
}
