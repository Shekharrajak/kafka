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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

class WorkerShareSinkTask extends WorkerTask<ConsumerRecord<byte[], byte[]>, SinkRecord> {
    private final ShareSinkTask task;
    private final ShareConsumer<byte[], byte[]> consumer;
    private final SinkConnectorConfig connectorConfig;
    private final ClusterConfigState configState;
    private final Plugin<Converter> keyConverter;
    private final Plugin<Converter> valueConverter;
    private final Plugin<HeaderConverter> headerConverter;
    private final SinkRecordConverter recordConverter;
    private final Duration acknowledgementTimeout;
    private final Set<TopicPartition> observedPartitions = new HashSet<>();
    private Map<String, String> taskConfig;
    private WorkerShareSinkTaskContext context;

    record RuntimeContext(ConnectorTaskId id,
                          TaskStatus.Listener statusListener,
                          TargetState initialState,
                          WorkerConfig workerConfig,
                          ClusterConfigState configState,
                          SinkConnectorConfig connectorConfig,
                          ConnectMetrics metrics,
                          ClassLoader loader,
                          Time time,
                          StatusBackingStore statusBackingStore,
                          TaskPluginsMetadata pluginsMetadata,
                          Function<ClassLoader, LoaderSwap> loaderSwapper) {
    }

    record Processing(Plugin<Converter> keyConverter,
                      Plugin<Converter> valueConverter,
                      Plugin<HeaderConverter> headerConverter,
                      ErrorHandlingMetrics errorMetrics,
                      TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformationChain,
                      RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> toleranceOperator,
                      Supplier<List<ErrorReporter<ConsumerRecord<byte[], byte[]>>>> errorReportersSupplier) {
    }

    WorkerShareSinkTask(ShareSinkTask task, ShareConsumer<byte[], byte[]> consumer,
                        RuntimeContext runtime, Processing processing) {
        super(runtime.id(), runtime.statusListener(), runtime.initialState(), runtime.loader(),
                runtime.metrics(), processing.errorMetrics(), processing.toleranceOperator(),
                processing.transformationChain(), processing.errorReportersSupplier(), runtime.time(),
                runtime.statusBackingStore(), runtime.pluginsMetadata(), runtime.loaderSwapper());
        this.task = task;
        this.consumer = consumer;
        this.connectorConfig = runtime.connectorConfig();
        this.configState = runtime.configState();
        this.keyConverter = processing.keyConverter();
        this.valueConverter = processing.valueConverter();
        this.headerConverter = processing.headerConverter();
        this.acknowledgementTimeout = Duration.ofMillis(runtime.workerConfig().getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG));
        this.recordConverter = new SinkRecordConverter(keyConverter, valueConverter, headerConverter,
                processing.toleranceOperator(), processing.transformationChain(), runtime.loaderSwapper(),
                topic -> {
                    if (runtime.workerConfig().getBoolean(WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG)) {
                        recordActiveTopic(topic);
                    }
                });
    }

    @Override
    public void initialize(TaskConfig config) {
        taskConfig = config.originalsStrings();
        context = new WorkerShareSinkTaskContext(() -> {
            Map<String, String> latest = configState.taskConfig(id);
            return latest == null ? taskConfig : latest;
        }, observedPartitions, pluginMetrics);
    }

    @Override
    protected void initializeAndStart() {
        SinkConnectorConfig.validate(connectorConfig.originalsStrings());
        task.initialize(context);
        task.start(taskConfig);
        consumer.subscribe(connectorConfig.getList(SinkConnectorConfig.TOPICS_CONFIG));
    }

    @Override
    protected void execute() {
        while (!isStopping()) {
            try {
                iteration();
            } catch (WakeupException e) {
                if (!isStopping()) {
                    throw e;
                }
            }
        }
    }

    void iteration() {
        if (shouldPause()) {
            onPause();
            try {
                if (awaitUnpause() && !isStopping()) {
                    onResume();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException("Interrupted while share task was paused", e);
            }
            return;
        }
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        if (records.isEmpty()) {
            return;
        }
        Set<TopicPartition> newlyObserved = new HashSet<>(records.partitions());
        newlyObserved.removeAll(observedPartitions);
        observedPartitions.addAll(newlyObserved);
        if (!newlyObserved.isEmpty()) {
            task.open(newlyObserved);
        }
        ShareSinkDelivery delivery = new ShareSinkDelivery(records, record -> {
            ProcessingContext<ConsumerRecord<byte[], byte[]>> processingContext = new ProcessingContext<>(record);
            SinkRecord converted = recordConverter.convert(processingContext, record);
            if (processingContext.failed()) {
                throw new ConnectException("Cannot acknowledge a record that failed conversion or transformation");
            }
            return converted;
        });
        long started = time.milliseconds();
        try {
            delivery.complete(task, consumer, acknowledgementTimeout);
            recordCommitSuccess(time.milliseconds() - started);
            recordBatch(records.count());
        } catch (RuntimeException e) {
            recordCommitFailure(time.milliseconds() - started);
            throw e;
        }
    }

    @Override
    public void stop() {
        super.stop();
        consumer.wakeup();
    }

    @Override
    protected void close() {
        Utils.closeQuietly(() -> task.close(Set.copyOf(observedPartitions)), "share task partitions");
        Utils.closeQuietly(task::stop, "share sink task");
        Utils.closeQuietly(consumer, "share consumer");
        Utils.closeQuietly(headerConverter, "header converter");
        Utils.closeQuietly(keyConverter, "key converter");
        Utils.closeQuietly(valueConverter, "value converter");
        Utils.closeQuietly(pluginMetrics, "plugin metrics");
    }

    @Override
    protected String taskVersion() {
        return task.version();
    }
}
