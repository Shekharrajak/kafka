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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class ShareIngressAssignment {
    private final String applicationId;
    private final Map<TopicPartition, TaskId> taskBySourcePartition;
    private final Map<TaskId, Set<TopicPartition>> ingressPartitionsByTask;
    private final Map<TopicPartition, TaskId> taskByIngressPartition;
    private final Map<TopicPartition, TopicPartition> sourceByIngressPartition;

    ShareIngressAssignment(final String applicationId, final Map<TaskId, Set<TopicPartition>> partitionsForTask) {
        this.applicationId = requireApplicationId(applicationId);
        this.taskBySourcePartition = new HashMap<>();
        Objects.requireNonNull(partitionsForTask, "partitionsForTask cannot be null");
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : partitionsForTask.entrySet()) {
            final TaskId taskId = Objects.requireNonNull(entry.getKey(), "taskId cannot be null");
            for (final TopicPartition sourcePartition : Objects.requireNonNull(entry.getValue(), "source partitions cannot be null")) {
                final TopicPartition nonNullSourcePartition = Objects.requireNonNull(sourcePartition, "sourcePartition cannot be null");
                final TaskId previousTask = taskBySourcePartition.putIfAbsent(nonNullSourcePartition, taskId);
                if (previousTask != null) {
                    throw new IllegalArgumentException("Source partition " + nonNullSourcePartition + " belongs to multiple tasks");
                }
            }
        }
        final Map<TaskId, Set<TopicPartition>> mutableIngressPartitionsByTask = new HashMap<>();
        taskByIngressPartition = new HashMap<>();
        sourceByIngressPartition = new HashMap<>();
        for (final Map.Entry<TopicPartition, TaskId> entry : taskBySourcePartition.entrySet()) {
            final TopicPartition ingressPartition = ingressPartition(entry.getKey());
            final TaskId targetTaskId = entry.getValue();
            final TaskId previousTask = taskByIngressPartition.putIfAbsent(ingressPartition, targetTaskId);
            if (previousTask != null) {
                throw new IllegalArgumentException("Ingress partition " + ingressPartition + " belongs to multiple source partitions");
            }
            sourceByIngressPartition.put(ingressPartition, entry.getKey());
            mutableIngressPartitionsByTask.computeIfAbsent(targetTaskId, ignored -> new HashSet<>()).add(ingressPartition);
        }
        ingressPartitionsByTask = immutablePartitionSets(mutableIngressPartitionsByTask);
    }

    TaskId targetTask(final TopicPartition sourcePartition) {
        final TopicPartition nonNullSourcePartition = Objects.requireNonNull(sourcePartition, "sourcePartition cannot be null");
        final TaskId taskId = taskBySourcePartition.get(nonNullSourcePartition);
        if (taskId == null) {
            throw new IllegalArgumentException("No task owns source partition " + nonNullSourcePartition);
        }
        return taskId;
    }

    TopicPartition ingressPartition(final TopicPartition sourcePartition) {
        final TopicPartition nonNullSourcePartition = Objects.requireNonNull(sourcePartition, "sourcePartition cannot be null");
        targetTask(nonNullSourcePartition);
        return new TopicPartition(ingressTopic(nonNullSourcePartition.topic()), nonNullSourcePartition.partition());
    }

    Map<TaskId, Set<TopicPartition>> ingressPartitionsByTask() {
        return ingressPartitionsByTask;
    }

    TaskId targetTaskForIngress(final TopicPartition ingressPartition) {
        final TopicPartition nonNullIngressPartition = Objects.requireNonNull(ingressPartition, "ingressPartition cannot be null");
        final TaskId taskId = taskByIngressPartition.get(nonNullIngressPartition);
        if (taskId == null) {
            throw new IllegalArgumentException("No task owns ingress partition " + nonNullIngressPartition);
        }
        return taskId;
    }

    TopicPartition sourcePartitionForIngress(final TopicPartition ingressPartition) {
        final TopicPartition nonNullIngressPartition = Objects.requireNonNull(ingressPartition, "ingressPartition cannot be null");
        final TopicPartition sourcePartition = sourceByIngressPartition.get(nonNullIngressPartition);
        if (sourcePartition == null) {
            throw new IllegalArgumentException("No source partition maps to ingress partition " + nonNullIngressPartition);
        }
        return sourcePartition;
    }

    private String ingressTopic(final String sourceTopic) {
        return applicationId + "-" + sourceTopic + "-share-ingress";
    }

    private static String requireApplicationId(final String applicationId) {
        if (applicationId == null || applicationId.isEmpty()) {
            throw new IllegalArgumentException("applicationId cannot be null or empty");
        }
        return applicationId;
    }

    private static Map<TaskId, Set<TopicPartition>> immutablePartitionSets(final Map<TaskId, Set<TopicPartition>> partitionsByTask) {
        final Map<TaskId, Set<TopicPartition>> immutablePartitionsByTask = new HashMap<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : partitionsByTask.entrySet()) {
            immutablePartitionsByTask.put(entry.getKey(), Set.copyOf(entry.getValue()));
        }
        return Map.copyOf(immutablePartitionsByTask);
    }
}
