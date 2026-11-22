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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

final class TransactionalShareSinkAdmin {
    private TransactionalShareSinkAdmin() {
    }

    static KafkaFuture<Void> validateAndRun(Function<Map<String, Object>, Admin> adminFactory,
                                            Map<String, Object> consumerConfig,
                                            Map<String, Object> producerConfig,
                                            Map<String, Object> adminConfig,
                                            String expectedClusterId,
                                            String groupId,
                                            long processingTimeoutMs,
                                            int timeoutMs,
                                            boolean validateGroupConfig,
                                            Function<Admin, KafkaFuture<Void>> operation) {
        KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        List<Admin> clients = new ArrayList<>();
        result.whenComplete((ignored, error) -> clients.forEach(client -> Utils.closeQuietly(() -> client.close(Duration.ZERO), "share sink validation admin")));
        try {
            Admin input = createAdmin(adminFactory, consumerConfig, "-input-validation");
            clients.add(input);
            Admin output = createAdmin(adminFactory, producerConfig, "-output-validation");
            clients.add(output);
            Admin control = createAdmin(adminFactory, adminConfig, "-share-validation");
            clients.add(control);
            DescribeClusterOptions clusterOptions = new DescribeClusterOptions().timeoutMs(timeoutMs);
            KafkaFuture<String> inputCluster = input.describeCluster(clusterOptions).clusterId();
            KafkaFuture<String> outputCluster = output.describeCluster(clusterOptions).clusterId();
            KafkaFuture<String> adminCluster = control.describeCluster(clusterOptions).clusterId();
            ConfigResource group = new ConfigResource(ConfigResource.Type.GROUP, groupId);
            KafkaFuture<Config> groupConfig = validateGroupConfig ? control.describeConfigs(List.of(group),
                new DescribeConfigsOptions().timeoutMs(timeoutMs)).values().get(group) : KafkaFuture.completedFuture(null);
            if (groupConfig == null) {
                throw new ConnectException("Share group configuration was not returned by the broker");
            }
            KafkaFuture.allOf(inputCluster, outputCluster, adminCluster, groupConfig).whenComplete((ignored, error) -> {
                if (result.isDone()) {
                    return;
                }
                if (error != null) {
                    result.completeExceptionally(error);
                    return;
                }
                try {
                    String inputId = inputCluster.getNow(null);
                    if (inputId == null || inputId.isBlank() || !inputId.equals(expectedClusterId) || !inputId.equals(outputCluster.getNow(null))
                        || !inputId.equals(adminCluster.getNow(null))) {
                        throw new ConnectException("Transactional share sinks require the effective consumer, producer and admin clients to use the worker config-topic Kafka cluster");
                    }
                    if (validateGroupConfig) {
                        validateGroup(groupConfig.getNow(null), processingTimeoutMs);
                    }
                    operation.apply(control).whenComplete((value, failure) -> {
                        if (failure == null) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(failure);
                        }
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    result.completeExceptionally(e);
                } catch (RuntimeException | ExecutionException e) {
                    result.completeExceptionally(e);
                }
            });
        } catch (RuntimeException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    private static Admin createAdmin(Function<Map<String, Object>, Admin> factory, Map<String, Object> config, String suffix) {
        Map<String, Object> validationConfig = new HashMap<>();
        config.forEach((key, value) -> {
            if (AdminClientConfig.configNames().contains(key) || key.equals("config.providers") || key.startsWith("config.providers.")) {
                validationConfig.put(key, value);
            }
        });
        validationConfig.put("client.id", config.getOrDefault("client.id", "connect-share-sink") + suffix);
        return factory.apply(validationConfig);
    }

    private static void validateGroup(Config config, long processingTimeoutMs) {
        ConfigEntry isolation = config == null ? null : config.get("share.isolation.level");
        if (isolation == null || !"read_committed".equals(isolation.value())) {
            throw new ConnectException("Transactional share sinks require the source group's share.isolation.level=read_committed");
        }
        ConfigEntry lockDuration = config.get("share.record.lock.duration.ms");
        long minimumLockDurationMs = processingTimeoutMs + Math.max(1000, processingTimeoutMs / 10);
        if (lockDuration == null || Long.parseLong(lockDuration.value()) <= minimumLockDurationMs) {
            throw new ConnectException("The source group's share.record.lock.duration.ms must exceed the processing timeout and recovery margin of " + minimumLockDurationMs + "ms");
        }
    }
}
