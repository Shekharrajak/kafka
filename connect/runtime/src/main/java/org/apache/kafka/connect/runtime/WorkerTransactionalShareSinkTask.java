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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.sink.TransactionalShareSinkTask;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;

final class WorkerTransactionalShareSinkTask extends WorkerTask<ConsumerRecord<byte[], byte[]>, SinkRecord> {
    private final TransactionalShareSinkTask task;
    private final Resources resources;
    private final ShareConsumer<byte[], byte[]> consumer;
    private final KafkaProducer<byte[], byte[]> producer;
    private final Runnable preProducerCheck;
    private final Runnable postProducerCheck;
    private final SinkRecordConverter recordConverter;
    private final long processingTimeoutMs;
    private final int maxOutputRecords;
    private final long maxOutputBytes;
    private final AtomicBoolean producerClosed = new AtomicBoolean();
    private final String version;
    private volatile BatchOutput activeOutput;
    private Map<String, String> taskConfig;

    @SuppressWarnings("ParameterNumber")
    WorkerTransactionalShareSinkTask(ConnectorTaskId id, TransactionalShareSinkTask task,
                                    TaskStatus.Listener statusListener, TargetState initialState,
                                    Resources resources, ShareConsumer<byte[], byte[]> consumer,
                                    KafkaProducer<byte[], byte[]> producer, ClassLoader loader, Time time,
                                    Runnable preProducerCheck, Runnable postProducerCheck,
                                    long processingTimeoutMs, int maxOutputRecords, long maxOutputBytes) {
        super(id, statusListener, initialState, loader, resources.connectMetrics(), resources.errorMetrics(),
                resources.retryWithToleranceOperator(), resources.transformationChain(), resources.reporters(),
                time, resources.statusBackingStore(), resources.pluginsMetadata(), resources.loaderSwapper());
        if (processingTimeoutMs <= 0 || maxOutputRecords <= 0 || maxOutputBytes <= 0) {
            throw new IllegalArgumentException("Transactional share sink limits must be positive");
        }
        this.task = task;
        this.resources = resources;
        this.consumer = consumer;
        this.producer = producer;
        this.preProducerCheck = preProducerCheck;
        this.postProducerCheck = postProducerCheck;
        this.processingTimeoutMs = processingTimeoutMs;
        this.maxOutputRecords = maxOutputRecords;
        this.maxOutputBytes = maxOutputBytes;
        this.version = task.version();
        this.recordConverter = new SinkRecordConverter(id, resources.keyConverter(), resources.valueConverter(),
                resources.headerConverter(), transformationChain, retryWithToleranceOperator, pluginLoaderSwapper,
                resources.workerConfig().getBoolean(TOPIC_TRACKING_ENABLE_CONFIG) ? this::recordActiveTopic : topic -> { });
    }

    @Override
    public void initialize(TaskConfig config) {
        taskConfig = Map.copyOf(config.originalsStrings());
    }

    @Override
    protected void initializeAndStart() {
        preProducerCheck.run();
        ensureRunning();
        producer.initTransactions();
        postProducerCheck.run();
        ensureRunning();
        consumer.setTransactionContextProvider(producer);
        if (!SinkConnectorConfig.hasTopicsConfig(taskConfig)) {
            throw new ConnectException("Transactional share sinks require an explicit topic list");
        }
        consumer.subscribe(SinkConnectorConfig.parseTopicsList(taskConfig));
        task.initialize(new Context());
        task.start(taskConfig);
    }

    @Override
    protected void execute() {
        while (!isStopping() && !isCancelled()) {
            if (shouldPause()) {
                onPause();
                try {
                    if (!awaitUnpause()) return;
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new ConnectException("Interrupted while awaiting task resumption", error);
                }
                onResume();
            }
            try {
                iteration();
            } catch (WakeupException ignored) {
                continue;
            }
        }
    }

    void iteration() {
        ensureRunning();
        long started = time.hiResClockMs();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(Math.min(100, processingTimeoutMs)));
        if (records.isEmpty()) return;
        processUnit(records, started);
    }

    private void processUnit(ConsumerRecords<byte[], byte[]> records, long started) {
        BatchOutput output = null;
        long commitStarted = -1;
        boolean transactionOpen = false;
        boolean commitAttempted = false;
        try {
            List<SinkRecord> prepared = new ArrayList<>(records.count());
            List<ConsumerRecord<byte[], byte[]>> originals = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> original : records) {
                checkBudget(started);
                ProcessingContext<ConsumerRecord<byte[], byte[]>> context = new ProcessingContext<>(original);
                SinkRecord converted = recordConverter.convert(context, original);
                if (context.failed()) {
                    throw new ConnectException("Transactional share sink conversion failed", context.error());
                }
                originals.add(original);
                if (converted != null) prepared.add(converted);
            }
            checkBudget(started);
            producer.beginTransaction();
            transactionOpen = true;
            output = new BatchOutput(started);
            activeOutput = output;
            try {
                if (!prepared.isEmpty()) task.put(List.copyOf(prepared), output);
            } finally {
                output.revoke();
                activeOutput = null;
            }
            output.awaitDelivery();
            for (ConsumerRecord<byte[], byte[]> original : originals) {
                checkBudget(started);
                consumer.acknowledge(original, AcknowledgeType.ACCEPT);
            }
            checkBudget(started);
            commitStarted = time.hiResClockMs();
            commitAttempted = true;
            producer.commitTransaction();
            transactionOpen = false;
            commitAttempted = false;
            recordCommitSuccess(time.hiResClockMs() - commitStarted);
            recordBatch(originals.size());
        } catch (Throwable failure) {
            if (output != null) output.revoke();
            activeOutput = null;
            if (commitAttempted) {
                forceCloseProducer();
            } else {
                abortBeforeCommit(transactionOpen, failure);
            }
            if (commitStarted >= 0) recordCommitFailure(time.hiResClockMs() - commitStarted);
            if (failure instanceof Error) throw (Error) failure;
            throw new ConnectException("Transactional share sink poll unit failed", failure);
        }
    }

    private void ensureRunning() {
        if (isStopping() || isCancelled() || producerClosed.get()) {
            throw new ConnectException("Transactional share sink is stopping");
        }
    }

    private long checkBudget(long started) {
        ensureRunning();
        long remaining = processingTimeoutMs - (time.hiResClockMs() - started);
        if (remaining <= 0) throw new ConnectException("Transactional share sink processing budget expired");
        return remaining;
    }

    private void abortBeforeCommit(boolean transactionOpen, Throwable failure) {
        if (!transactionOpen || producerClosed.get()) return;
        try {
            producer.abortTransaction();
        } catch (Throwable abortFailure) {
            if (abortFailure != failure) failure.addSuppressed(abortFailure);
            forceCloseProducer();
        }
    }

    private void forceCloseProducer() {
        if (producerClosed.compareAndSet(false, true)) {
            Utils.closeQuietly(() -> producer.close(Duration.ZERO), "transactional share sink producer");
        }
    }

    @Override
    public void stop() {
        super.stop();
        BatchOutput output = activeOutput;
        if (output != null) output.revoke();
        consumer.wakeup();
    }

    @Override
    public void cancel() {
        super.stop();
        super.cancel();
        BatchOutput output = activeOutput;
        if (output != null) output.revoke();
        consumer.wakeup();
        forceCloseProducer();
    }

    @Override
    public void transitionTo(TargetState state) {
        super.transitionTo(state);
        consumer.wakeup();
    }

    @Override
    protected void close() {
        BatchOutput output = activeOutput;
        if (output != null) output.revoke();
        forceCloseProducer();
        Utils.closeQuietly(() -> consumer.close(Duration.ofMillis(processingTimeoutMs)), "transactional share sink consumer");
        Utils.closeQuietly(task::stop, "transactional share sink task");
        Utils.closeQuietly(resources.headerConverter(), "header converter");
        Utils.closeQuietly(resources.keyConverter(), "key converter");
        Utils.closeQuietly(resources.valueConverter(), "value converter");
        Utils.closeQuietly(pluginMetrics, "plugin metrics");
    }

    @Override
    protected String taskVersion() {
        return version;
    }

    private final class BatchOutput implements TransactionalShareSinkTask.Output {
        private final Thread owner = Thread.currentThread();
        private final long started;
        private final List<Future<RecordMetadata>> deliveries = new ArrayList<>();
        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        private volatile boolean active = true;
        private final AtomicLong bytes = new AtomicLong();

        private BatchOutput(long started) {
            this.started = started;
        }

        @Override
        public void send(ProducerRecord<byte[], byte[]> record) {
            try {
                if (Thread.currentThread() != owner || !active || activeOutput != this) {
                    throw new ConnectException("Output is only valid on the task thread during its put call");
                }
                checkBudget(started);
                Objects.requireNonNull(record, "record");
                if (deliveries.size() >= maxOutputRecords) throw new ConnectException("Transactional output record limit exceeded");
                long recordBytes = 64L + record.topic().getBytes(StandardCharsets.UTF_8).length
                        + size(record.key()) + size(record.value());
                List<Header> headers = new ArrayList<>();
                for (Header header : record.headers()) {
                    recordBytes += header.key().getBytes(StandardCharsets.UTF_8).length + size(header.value()) + 16L;
                    ensureCapacity(recordBytes);
                    headers.add(new RecordHeader(header.key(), copy(header.value())));
                }
                ensureCapacity(recordBytes);
                bytes.addAndGet(recordBytes);
                ProducerRecord<byte[], byte[]> copy = new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                        copy(record.key()), copy(record.value()), headers);
                deliveries.add(Objects.requireNonNull(producer.send(copy), "producer delivery future"));
            } catch (RuntimeException error) {
                failure.compareAndSet(null, error);
                throw error;
            }
        }

        private void ensureCapacity(long recordBytes) {
            if (recordBytes > maxOutputBytes - bytes.get()) throw new ConnectException("Transactional output byte limit exceeded");
        }

        private void revoke() {
            active = false;
        }

        private void awaitDelivery() {
            throwIfFailed();
            for (Future<RecordMetadata> delivery : deliveries) {
                try {
                    delivery.get(checkBudget(started), TimeUnit.MILLISECONDS);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new ConnectException("Interrupted while awaiting transactional output", error);
                } catch (ExecutionException | TimeoutException error) {
                    throw new ConnectException("Transactional output delivery failed", error);
                }
            }
            throwIfFailed();
        }

        private void throwIfFailed() {
            RuntimeException error = failure.get();
            if (error != null) throw error;
        }
    }

    private static int size(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }

    private static byte[] copy(byte[] bytes) {
        return bytes == null ? null : bytes.clone();
    }

    private final class Context implements SinkTaskContext {
        @Override
        public Map<String, String> configs() {
            Map<String, String> latest = resources.configState().taskConfig(id);
            return latest == null ? taskConfig : latest;
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            throw unsupported("offset resets");
        }

        @Override
        public void offset(TopicPartition partition, long offset) {
            throw unsupported("offset resets");
        }

        @Override
        public void timeout(long timeoutMs) {
            throw unsupported("connector-controlled retries");
        }

        @Override
        public Set<TopicPartition> assignment() {
            throw unsupported("partition assignment");
        }

        @Override
        public void pause(TopicPartition... partitions) {
            throw unsupported("partition pause");
        }

        @Override
        public void resume(TopicPartition... partitions) {
            throw unsupported("partition resume");
        }

        @Override
        public void requestCommit() {
            throw unsupported("offset commits");
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            throw unsupported("errant record reporting");
        }

        @Override
        public PluginMetrics pluginMetrics() {
            return pluginMetrics;
        }

        private UnsupportedOperationException unsupported(String operation) {
            return new UnsupportedOperationException("Transactional share sinks do not support " + operation);
        }
    }

    record Resources(WorkerConfig workerConfig, ClusterConfigState configState, ConnectMetrics connectMetrics,
                     Plugin<Converter> keyConverter, Plugin<Converter> valueConverter, ErrorHandlingMetrics errorMetrics,
                     Plugin<HeaderConverter> headerConverter,
                     TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformationChain,
                     RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator,
                     StatusBackingStore statusBackingStore,
                     Supplier<List<ErrorReporter<ConsumerRecord<byte[], byte[]>>>> reporters,
                     TaskPluginsMetadata pluginsMetadata, Function<ClassLoader, LoaderSwap> loaderSwapper) {
    }
}
