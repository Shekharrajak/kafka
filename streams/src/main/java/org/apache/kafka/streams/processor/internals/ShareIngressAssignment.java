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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class ShareIngressAssignment {
    private final String applicationId;
    private final Map<TopicPartition, TaskId> taskBySourcePartition;

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

    private String ingressTopic(final String sourceTopic) {
        return applicationId + "-" + sourceTopic + "-share-ingress";
    }

    private static String requireApplicationId(final String applicationId) {
        if (applicationId == null || applicationId.isEmpty()) {
            throw new IllegalArgumentException("applicationId cannot be null or empty");
        }
        return applicationId;
    }
}
