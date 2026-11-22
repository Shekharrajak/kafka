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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.connect.errors.ConnectException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransactionalShareSinkAdminTest {
    private static final String GROUP = "dedicated-input";
    private final List<Map<String, Object>> configs = new ArrayList<>();

    @Test
    public void testWaitsForEffectiveClusterIdentitiesAndClosesValidationClients() throws Exception {
        KafkaFutureImpl<String> inputId = new KafkaFutureImpl<>();
        List<Admin> admins = List.of(admin(inputId, validGroup()), admin(KafkaFuture.completedFuture("cluster"), validGroup()),
            admin(KafkaFuture.completedFuture("cluster"), validGroup()));
        AtomicBoolean operation = new AtomicBoolean();
        KafkaFuture<Void> result = validate(admins, true, control -> {
            assertEquals(admins.get(2), control);
            operation.set(true);
            return KafkaFuture.completedFuture(null);
        });
        assertFalse(result.isDone());
        assertFalse(operation.get());
        inputId.complete("cluster");
        result.get();
        assertTrue(operation.get());
        assertEquals("input:9092", configs.get(0).get("bootstrap.servers"));
        assertEquals("output:9092", configs.get(1).get("bootstrap.servers"));
        assertEquals("admin:9092", configs.get(2).get("bootstrap.servers"));
        assertEquals("PLAINTEXT", configs.get(0).get("security.protocol"));
        assertFalse(configs.get(1).containsKey("transactional.id"));
        admins.forEach(client -> verify(client).close(Duration.ZERO));
    }

    @Test
    public void testCrossClusterOutputCannotStartOrFence() {
        List<Admin> admins = List.of(admin(KafkaFuture.completedFuture("cluster"), validGroup()),
            admin(KafkaFuture.completedFuture("other"), validGroup()), admin(KafkaFuture.completedFuture("cluster"), validGroup()));
        AtomicBoolean operation = new AtomicBoolean();
        KafkaFuture<Void> result = validate(admins, false, control -> {
            operation.set(true);
            return KafkaFuture.completedFuture(null);
        });
        ExecutionException error = assertThrows(ExecutionException.class, result::get);
        assertInstanceOf(ConnectException.class, error.getCause());
        assertFalse(operation.get());
        admins.forEach(client -> verify(client).close(Duration.ZERO));
    }

    @Test
    public void testMatchingDataClustersCannotMigrateAwayFromWorkerControlCluster() {
        List<Admin> admins = List.of(admin(KafkaFuture.completedFuture("other"), validGroup()),
            admin(KafkaFuture.completedFuture("other"), validGroup()), admin(KafkaFuture.completedFuture("other"), validGroup()));
        AtomicBoolean operation = new AtomicBoolean();
        KafkaFuture<Void> result = validate(admins, false, control -> {
            operation.set(true);
            return KafkaFuture.completedFuture(null);
        });
        assertThrows(ExecutionException.class, result::get);
        assertFalse(operation.get());
        admins.forEach(client -> verify(client).close(Duration.ZERO));
    }

    @ParameterizedTest
    @CsvSource({"read_uncommitted,60000", "read_committed,10000"})
    public void testStartupRejectsUnsafeGroupSettings(String isolation, long lockDurationMs) {
        Config group = group(isolation, lockDurationMs);
        List<Admin> admins = List.of(admin(KafkaFuture.completedFuture("cluster"), group),
            admin(KafkaFuture.completedFuture("cluster"), group), admin(KafkaFuture.completedFuture("cluster"), group));
        AtomicBoolean operation = new AtomicBoolean();
        KafkaFuture<Void> result = validate(admins, true, control -> {
            operation.set(true);
            return KafkaFuture.completedFuture(null);
        });
        ExecutionException error = assertThrows(ExecutionException.class, result::get);
        assertInstanceOf(ConnectException.class, error.getCause());
        assertFalse(operation.get());
    }

    @Test
    public void testFencingDoesNotRequireExistingOrSafelyConfiguredGroup() throws Exception {
        List<Admin> admins = List.of(admin(KafkaFuture.completedFuture("cluster"), null),
            admin(KafkaFuture.completedFuture("cluster"), null), admin(KafkaFuture.completedFuture("cluster"), null));
        AtomicBoolean fenced = new AtomicBoolean();
        validate(admins, false, control -> {
            fenced.set(true);
            return KafkaFuture.completedFuture(null);
        }).get();
        assertTrue(fenced.get());
        verify(admins.get(2), never()).describeConfigs(anyCollection(), any(DescribeConfigsOptions.class));
    }

    @Test
    public void testCancellationClosesClientsWithoutWaitingForMetadata() {
        KafkaFutureImpl<String> inputId = new KafkaFutureImpl<>();
        List<Admin> admins = List.of(admin(inputId, validGroup()),
            admin(KafkaFuture.completedFuture("cluster"), validGroup()), admin(KafkaFuture.completedFuture("cluster"), validGroup()));
        AtomicBoolean operation = new AtomicBoolean();
        KafkaFuture<Void> result = validate(admins, true, control -> {
            operation.set(true);
            return KafkaFuture.completedFuture(null);
        });
        assertFalse(result.isDone());
        result.cancel(true);
        assertTrue(result.isCancelled());
        inputId.complete("cluster");
        assertFalse(operation.get());
        admins.forEach(client -> verify(client).close(Duration.ZERO));
    }

    private KafkaFuture<Void> validate(List<Admin> admins, boolean checkGroup, Function<Admin, KafkaFuture<Void>> operation) {
        AtomicInteger index = new AtomicInteger();
        return TransactionalShareSinkAdmin.validateAndRun(config -> {
            configs.add(config);
            return admins.get(index.getAndIncrement());
        }, Map.of("bootstrap.servers", "input:9092", "security.protocol", "PLAINTEXT"),
            Map.of("bootstrap.servers", "output:9092", "transactional.id", "private-id"),
            Map.of("bootstrap.servers", "admin:9092"), "cluster", GROUP, 10000, 30000, checkGroup, operation);
    }

    private Admin admin(KafkaFuture<String> clusterId, Config group) {
        Admin result = mock(Admin.class);
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(cluster.clusterId()).thenReturn(clusterId);
        when(result.describeCluster(any(DescribeClusterOptions.class))).thenReturn(cluster);
        if (group != null) {
            DescribeConfigsResult described = mock(DescribeConfigsResult.class);
            when(described.values()).thenReturn(Map.of(new ConfigResource(ConfigResource.Type.GROUP, GROUP), KafkaFuture.completedFuture(group)));
            when(result.describeConfigs(anyCollection(), any(DescribeConfigsOptions.class))).thenReturn(described);
        }
        return result;
    }

    private Config validGroup() {
        return group("read_committed", 60000);
    }

    private Config group(String isolation, long lockDurationMs) {
        return new Config(List.of(new ConfigEntry("share.isolation.level", isolation),
            new ConfigEntry("share.record.lock.duration.ms", Long.toString(lockDurationMs))));
    }
}
