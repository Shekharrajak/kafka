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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest;
import org.apache.kafka.connect.runtime.isolation.TestPlugins;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.sink.TransactionalShareSinkTask;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerTransactionalShareSinkTaskTest {
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("transactional", 0);
    private static final ConsumerRecord<byte[], byte[]> INPUT = new ConsumerRecord<>("input", 0, 10L, new byte[]{1}, new byte[]{2});
    private static final ProducerRecord<byte[], byte[]> OUTPUT = new ProducerRecord<>("output", new byte[]{3}, new byte[]{4});
    private static final long PROCESSING_TIMEOUT_MS = 1000;
    private final ShareConsumer<byte[], byte[]> consumer = mock(ShareConsumer.class);
    private final KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
    private final TransactionalShareSinkTask task = mock(TransactionalShareSinkTask.class);
    private final Converter keyConverter = mock(Converter.class);
    private final Converter valueConverter = mock(Converter.class);
    private final HeaderConverter headerConverter = mock(HeaderConverter.class);
    private final TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformations = mock(TransformationChain.class);
    private final Runnable preCheck = mock(Runnable.class);
    private final Runnable postCheck = mock(Runnable.class);
    private final MockTime time = new MockTime();
    private MockConnectMetrics metrics;
    private WorkerTransactionalShareSinkTask worker;

    @BeforeEach
    void setUp() {
        metrics = new MockConnectMetrics(time);
        when(task.version()).thenReturn("1");
        when(keyConverter.toConnectData(anyString(), any(Headers.class), any(byte[].class)))
                .thenReturn(new SchemaAndValue(Schema.BYTES_SCHEMA, INPUT.key()));
        when(valueConverter.toConnectData(anyString(), any(Headers.class), any(byte[].class)))
                .thenReturn(new SchemaAndValue(Schema.BYTES_SCHEMA, INPUT.value()));
        when(transformations.apply(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(new TopicPartition("input", 0), List.of(INPUT)), Map.of()));
        when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(null));
        createWorker(10, 1024);
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            worker.doClose();
            worker.removeMetrics();
        }
        if (metrics != null) metrics.stop();
    }

    @Test
    void testStartupValidatesOwnershipBeforeAndAfterProducerInitialization() {
        worker.initializeAndStart();

        InOrder order = inOrder(preCheck, producer, postCheck, consumer, task);
        order.verify(preCheck).run();
        order.verify(producer).initTransactions();
        order.verify(postCheck).run();
        order.verify(consumer).setTransactionContextProvider(producer);
        order.verify(consumer).subscribe(List.of("input"));
        order.verify(task).initialize(any());
        order.verify(task).start(any());
        verify(consumer, never()).poll(any());
    }

    @Test
    void testOutputDeliveryCompletesBeforeAcknowledgementsAndCommit() throws Exception {
        Future<RecordMetadata> delivery = mock(Future.class);
        when(producer.send(any())).thenReturn(delivery);
        doAnswer(invocation -> {
            invocation.<TransactionalShareSinkTask.Output>getArgument(1).send(OUTPUT);
            return null;
        }).when(task).put(anyCollection(), any());

        worker.iteration();

        InOrder order = inOrder(producer, task, delivery, consumer);
        order.verify(consumer).poll(any());
        order.verify(producer).beginTransaction();
        order.verify(task).put(anyCollection(), any());
        order.verify(producer).send(any());
        order.verify(delivery).get(anyLong(), eq(TimeUnit.MILLISECONDS));
        order.verify(consumer).acknowledge(INPUT, AcknowledgeType.ACCEPT);
        order.verify(producer).commitTransaction();
        verify(consumer, never()).commitSync(any());
    }

    @Test
    void testAllFilteredRecordsStillCommitAnAcknowledgementOnlyTransaction() {
        when(transformations.apply(any(), any())).thenReturn(null);

        worker.iteration();

        verify(task, never()).put(anyCollection(), any());
        verify(producer, never()).send(any());
        InOrder order = inOrder(producer, consumer);
        order.verify(producer).beginTransaction();
        order.verify(consumer).acknowledge(INPUT, AcknowledgeType.ACCEPT);
        order.verify(producer).commitTransaction();
    }

    @Test
    void testOutputDeliveryFailureAbortsBeforeAcknowledgingInput() throws Exception {
        Future<RecordMetadata> delivery = mock(Future.class);
        when(producer.send(any())).thenReturn(delivery);
        when(delivery.get(anyLong(), eq(TimeUnit.MILLISECONDS))).thenThrow(new ExecutionException(new IllegalStateException("send")));
        doAnswer(invocation -> {
            invocation.<TransactionalShareSinkTask.Output>getArgument(1).send(OUTPUT);
            return null;
        }).when(task).put(anyCollection(), any());

        assertThrows(ConnectException.class, worker::iteration);

        verify(producer).abortTransaction();
        verify(producer, never()).commitTransaction();
        verify(consumer, never()).acknowledge(any(), any());
    }

    @Test
    void testUnknownCommitForceClosesWithoutOppositeDecision() {
        doThrow(new TimeoutException("commit outcome unknown")).when(producer).commitTransaction();

        assertThrows(ConnectException.class, worker::iteration);

        verify(consumer).acknowledge(INPUT, AcknowledgeType.ACCEPT);
        verify(producer).close(Duration.ZERO);
        verify(producer, never()).abortTransaction();
        verify(consumer, never()).commitSync(any());
    }

    @Test
    void testConversionBudgetExpiresBeforeBeginningTransaction() {
        when(keyConverter.toConnectData(anyString(), any(Headers.class), any(byte[].class))).thenAnswer(invocation -> {
            time.sleep(PROCESSING_TIMEOUT_MS + 1);
            return new SchemaAndValue(Schema.BYTES_SCHEMA, INPUT.key());
        });

        assertThrows(ConnectException.class, worker::iteration);

        verify(producer, never()).beginTransaction();
        verify(consumer, never()).acknowledge(any(), any());
    }

    @Test
    void testOutputHandleIsInvalidAfterPutReturns() {
        AtomicReference<TransactionalShareSinkTask.Output> handle = new AtomicReference<>();
        doAnswer(invocation -> {
            handle.set(invocation.getArgument(1));
            return null;
        }).when(task).put(anyCollection(), any());
        worker.iteration();

        assertThrows(ConnectException.class, () -> handle.get().send(OUTPUT));

        verify(producer, never()).send(any());
    }

    @Test
    void testOutOfThreadOutputCannotBeHiddenByPlugin() {
        AtomicReference<Throwable> denied = new AtomicReference<>();
        doAnswer(invocation -> {
            TransactionalShareSinkTask.Output output = invocation.getArgument(1);
            Thread thread = new Thread(() -> {
                try {
                    output.send(OUTPUT);
                } catch (Throwable failure) {
                    denied.set(failure);
                }
            });
            thread.start();
            thread.join();
            return null;
        }).when(task).put(anyCollection(), any());

        assertThrows(ConnectException.class, worker::iteration);

        assertTrue(denied.get() instanceof ConnectException);
        verify(producer, never()).send(any());
        verify(producer).abortTransaction();
        verify(consumer, never()).acknowledge(any(), any());
    }

    @Test
    void testOutputLimitFailureCannotBeHiddenByPlugin() {
        doAnswer(invocation -> {
            TransactionalShareSinkTask.Output output = invocation.getArgument(1);
            for (int index = 0; index < 10; index++) output.send(OUTPUT);
            assertThrows(ConnectException.class, () -> output.send(OUTPUT));
            return null;
        }).when(task).put(anyCollection(), any());

        assertThrows(ConnectException.class, worker::iteration);

        verify(producer).abortTransaction();
        verify(consumer, never()).acknowledge(any(), any());
    }

    @Test
    void testBudgetExpiryAfterOutputSendAbortsBeforeInputAcknowledgement() {
        doAnswer(invocation -> {
            invocation.<TransactionalShareSinkTask.Output>getArgument(1).send(OUTPUT);
            time.sleep(PROCESSING_TIMEOUT_MS + 1);
            return null;
        }).when(task).put(anyCollection(), any());

        assertThrows(ConnectException.class, worker::iteration);

        verify(producer).abortTransaction();
        verify(producer, never()).commitTransaction();
        verify(consumer, never()).acknowledge(any(), any());
    }

    @Test
    void testDelayedControlWakeupReloopsAfterResumption() {
        when(consumer.poll(any())).thenThrow(new WakeupException()).thenAnswer(invocation -> {
            worker.stop();
            return ConsumerRecords.empty();
        });

        worker.execute();

        verify(consumer, times(2)).poll(any());
        verify(producer, never()).beginTransaction();
    }

    @Test
    void testContextRejectsOffsetAssignmentAndReporterControls() {
        worker.initializeAndStart();
        ArgumentCaptor<SinkTaskContext> context = ArgumentCaptor.forClass(SinkTaskContext.class);
        verify(task).initialize(context.capture());

        assertThrows(UnsupportedOperationException.class, () -> context.getValue().offset(Map.of()));
        assertThrows(UnsupportedOperationException.class, () -> context.getValue().offset(new TopicPartition("input", 0), 0));
        assertThrows(UnsupportedOperationException.class, context.getValue()::assignment);
        assertThrows(UnsupportedOperationException.class, () -> context.getValue().pause(new TopicPartition("input", 0)));
        assertThrows(UnsupportedOperationException.class, () -> context.getValue().resume(new TopicPartition("input", 0)));
        assertThrows(UnsupportedOperationException.class, () -> context.getValue().timeout(1));
        assertThrows(UnsupportedOperationException.class, context.getValue()::requestCommit);
        assertThrows(UnsupportedOperationException.class, context.getValue()::errantRecordReporter);
        assertNotNull(context.getValue().pluginMetrics());
        assertEquals("input", context.getValue().configs().get("topics"));
    }

    private void createWorker(int maxOutputs, long maxBytes) {
        WorkerConfig config = mock(WorkerConfig.class);
        Plugin<Converter> key = metrics.wrap(keyConverter, TASK_ID, true);
        Plugin<Converter> value = metrics.wrap(valueConverter, TASK_ID, false);
        Plugin<HeaderConverter> header = metrics.wrap(headerConverter, TASK_ID);
        WorkerTransactionalShareSinkTask.Resources resources = new WorkerTransactionalShareSinkTask.Resources(config,
                ClusterConfigState.EMPTY, metrics, key, value, mock(ErrorHandlingMetrics.class), header,
                transformations, RetryWithToleranceOperatorTest.noneOperator(), mock(StatusBackingStore.class),
                List::of, null, TestPlugins.noOpLoaderSwap());
        worker = new WorkerTransactionalShareSinkTask(TASK_ID, task, mock(TaskStatus.Listener.class), TargetState.STARTED,
                resources, consumer, producer, getClass().getClassLoader(), time, preCheck, postCheck,
                PROCESSING_TIMEOUT_MS, maxOutputs, maxBytes);
        worker.initialize(new TaskConfig(Map.of("topics", "input", TaskConfig.TASK_CLASS_CONFIG, TransactionalShareSinkTask.class.getName())));
    }
}
