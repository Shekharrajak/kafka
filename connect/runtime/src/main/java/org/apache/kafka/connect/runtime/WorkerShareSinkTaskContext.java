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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

class WorkerShareSinkTaskContext implements SinkTaskContext {
    private final Supplier<Map<String, String>> configs;
    private final Set<TopicPartition> observedPartitions;
    private final PluginMetrics pluginMetrics;

    WorkerShareSinkTaskContext(Supplier<Map<String, String>> configs,
                              Set<TopicPartition> observedPartitions, PluginMetrics pluginMetrics) {
        this.configs = configs;
        this.observedPartitions = observedPartitions;
        this.pluginMetrics = pluginMetrics;
    }

    @Override
    public Map<String, String> configs() {
        return Map.copyOf(configs.get());
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Set.copyOf(observedPartitions);
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        throw new UnsupportedOperationException("Share consumption does not support seeking offsets");
    }

    @Override
    public void offset(TopicPartition partition, long offset) {
        throw new UnsupportedOperationException("Share consumption does not support seeking offsets");
    }

    @Override
    public void pause(TopicPartition... partitions) {
        throw new UnsupportedOperationException("Share consumption does not support partition pause");
    }

    @Override
    public void resume(TopicPartition... partitions) {
        throw new UnsupportedOperationException("Share consumption does not support partition resume");
    }

    @Override
    public void timeout(long timeoutMs) {
    }

    @Override
    public void requestCommit() {
    }

    @Override
    public PluginMetrics pluginMetrics() {
        return pluginMetrics;
    }
}
