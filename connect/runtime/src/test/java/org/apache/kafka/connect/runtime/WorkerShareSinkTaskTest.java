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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.TestPlugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerShareSinkTaskTest {
    private final ConnectorTaskId id = new ConnectorTaskId("share-test", 0);
    private final TopicPartition partition = new TopicPartition("events", 0);
    private final ShareSinkTask task = mock(ShareSinkTask.class);
    @SuppressWarnings("unchecked")
    private final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
    @SuppressWarnings("unchecked")
    private final TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformations = mock(TransformationChain.class);
    private final TaskStatus.Listener listener = mock(TaskStatus.Listener.class);
    private MockConnectMetrics metrics;
    private WorkerShareSinkTask worker;
    private Map<String, String> props;

    @BeforeEach
    void setup() {
        MockTime time = new MockTime();
        props = Map.of("name", "share-test", "connector.class", SinkConnector.class.getName(),
                "topics", "events", "consumer.type", "share");
        WorkerConfig config = new StandaloneConfig(Map.of("bootstrap.servers", "localhost:9092",
                "key.converter", StringConverter.class.getName(), "value.converter", StringConverter.class.getName(),
                "offset.storage.file.filename", "/tmp/share-connect-offsets"));
        metrics = new MockConnectMetrics(time);
        StringConverter key = new StringConverter();
        key.configure(Map.of(), true);
        StringConverter value = new StringConverter();
        value.configure(Map.of(), false);
        SimpleHeaderConverter header = new SimpleHeaderConverter();
        header.configure(Map.of());
        worker = new WorkerShareSinkTask(task, consumer,
                new WorkerShareSinkTask.RuntimeContext(id, listener, TargetState.STARTED, config,
                        ClusterConfigState.EMPTY, new SinkConnectorConfig(mock(Plugins.class), props), metrics,
                        getClass().getClassLoader(), time, mock(StatusBackingStore.class), null, TestPlugins.noOpLoaderSwap()),
                new WorkerShareSinkTask.Processing(metrics.wrap(key, id, true), metrics.wrap(value, id, false),
                        metrics.wrap(header, id), mock(ErrorHandlingMetrics.class), transformations,
                        RetryWithToleranceOperatorTest.noneOperator(), List::of));
        worker.initialize(new TaskConfig(Map.of(TaskConfig.TASK_CLASS_CONFIG, ShareSinkTask.class.getName())));
        when(transformations.apply(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
        when(consumer.commitSync(any(Duration.class))).thenReturn(Map.of());
    }

    @AfterEach
    void cleanup() {
        worker.close();
        worker.removeMetrics();
        metrics.stop();
    }

    private ConsumerRecords<byte[], byte[]> records() {
        return new ConsumerRecords<>(Map.of(partition, List.of(
                new ConsumerRecord<>("events", 0, 101, null, "value".getBytes(UTF_8)),
                new ConsumerRecord<>("events", 0, 100, null, "value".getBytes(UTF_8)))));
    }

    @Test
    void initializesTaskAndSubscribesUsingConnectorConfiguration() {
        worker.initializeAndStart();
        verify(consumer).subscribe(List.of("events"));
        ArgumentCaptor<SinkTaskContext> context = ArgumentCaptor.forClass(SinkTaskContext.class);
        verify(task).initialize(context.capture());
        assertEquals(ShareSinkTask.class.getName(), context.getValue().configs().get(TaskConfig.TASK_CLASS_CONFIG));
    }

    @Test
    void convertsRecordsAndOpensObservedPartitionsOnce() {
        when(consumer.poll(any(Duration.class))).thenReturn(records());
        worker.iteration();
        worker.iteration();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SinkRecord>> batch = ArgumentCaptor.forClass(Collection.class);
        verify(task, times(2)).put(batch.capture());
        verify(task).open(Set.of(partition));
        assertEquals(List.of(101L, 100L), batch.getValue().stream().map(SinkRecord::kafkaOffset).toList());
        assertEquals("value", batch.getValue().iterator().next().value());
    }

    @Test
    void writeFailureStopsProcessingWithoutAcknowledging() {
        when(consumer.poll(any(Duration.class))).thenReturn(records());
        doThrow(new RetriableException("Doris unavailable")).when(task).put(anyCollection());
        assertThrows(RetriableException.class, worker::iteration);
        verify(consumer, never()).acknowledge(any(), any());
        verify(consumer, never()).commitSync(any(Duration.class));
    }

    @Test
    void converterFailureDoesNotReachSinkOrAcknowledgements() {
        when(consumer.poll(any(Duration.class))).thenReturn(records());
        when(transformations.apply(any(), any())).thenThrow(new RetriableException("transform failed"));
        assertThrows(RetriableException.class, worker::iteration);
        verify(task, never()).put(anyCollection());
        verify(consumer, never()).acknowledge(any(), any());
    }

    @Test
    void deliberateFilterAcknowledgesOriginalRecords() {
        ConsumerRecords<byte[], byte[]> input = records();
        when(consumer.poll(any(Duration.class))).thenReturn(input);
        when(transformations.apply(any(), any())).thenReturn(null);
        worker.iteration();
        verify(task, never()).put(anyCollection());
        for (ConsumerRecord<byte[], byte[]> record : input) {
            verify(consumer).acknowledge(record, AcknowledgeType.ACCEPT);
        }
    }

    @Test
    void pausedTaskDoesNotPollUntilResumed() throws Exception {
        worker.transitionTo(TargetState.PAUSED);
        CompletableFuture<Void> iteration = CompletableFuture.runAsync(worker::iteration);
        try {
            verify(listener, timeout(1000)).onPause(id);
            verify(consumer, never()).poll(any(Duration.class));
            worker.transitionTo(TargetState.STARTED);
            iteration.get(5, TimeUnit.SECONDS);
            verify(listener).onResume(id);
        } finally {
            worker.stop();
        }
    }

    @Test
    void shutdownWakeupExitsExecution() {
        when(consumer.poll(any(Duration.class))).thenAnswer(invocation -> {
            worker.stop();
            throw new WakeupException();
        });
        worker.execute();
        verify(consumer).wakeup();
    }

    @Test
    void unexpectedWakeupFailsTask() {
        when(consumer.poll(any(Duration.class))).thenThrow(new WakeupException());
        assertThrows(WakeupException.class, worker::execute);
    }
}
