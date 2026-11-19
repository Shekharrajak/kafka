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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

final class NativeGroupMetrics {
    private final Admin admin;
    private volatile String snapshot = "";
    private volatile int classicMembers;
    private volatile int shareMembers;
    private int minimumClassicEligibility;
    private int minimumShareEligibility;
    private Map<Integer, Long> classicLag = Map.of();
    private Map<Integer, Long> shareLag = Map.of();

    NativeGroupMetrics(Admin admin) {
        this.admin = admin;
    }

    synchronized void refresh(String runId, String inputTopic, String classicGroup, String shareGroup) {
        List<TopicPartition> partitions = IntStream.range(0, 3).mapToObj(p -> new TopicPartition(inputTopic, p)).toList();
        StringBuilder result = new StringBuilder();
        String labels = "run_id=\"" + runId + "\"";
        try {
            ConsumerGroupDescription group = admin.describeConsumerGroups(List.of(classicGroup)).all().get(5, TimeUnit.SECONDS).get(classicGroup);
            classicMembers = group.members().size();
            minimumClassicEligibility = Integer.MAX_VALUE;
            sample(result, "demo_members", labels + ",mode=\"classic\"", classicMembers);
            for (TopicPartition partition : partitions) {
                long eligible = group.members().stream().filter(member -> member.assignment().topicPartitions().contains(partition)).count();
                minimumClassicEligibility = Math.min(minimumClassicEligibility, (int) eligible);
                sample(result, "demo_partition_eligible_members", labels + ",mode=\"classic\",partition=\"" + partition.partition() + "\"", eligible);
            }
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(classicGroup).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            partitions.forEach(partition -> request.put(partition, OffsetSpec.latest()));
            var ends = admin.listOffsets(request).all().get(5, TimeUnit.SECONDS);
            Map<Integer, Long> lag = new HashMap<>();
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata offset = committed.get(partition);
                if (offset != null) {
                    long remaining = Math.max(0, ends.get(partition).offset() - offset.offset());
                    lag.put(partition.partition(), remaining);
                    sample(result, "demo_source_lag", labels + ",mode=\"classic\",partition=\"" + partition.partition() + "\"",
                            remaining);
                }
            }
            classicLag = Map.copyOf(lag);
            sample(result, "demo_admin_ok", labels + ",mode=\"classic\"", 1);
        } catch (InterruptedException | ExecutionException | TimeoutException | RuntimeException e) {
            classicMembers = -1;
            minimumClassicEligibility = 0;
            classicLag = Map.of();
            sample(result, "demo_admin_ok", labels + ",mode=\"classic\"", 0);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            ShareGroupDescription group = admin.describeShareGroups(List.of(shareGroup)).all().get(5, TimeUnit.SECONDS).get(shareGroup);
            shareMembers = group.members().size();
            minimumShareEligibility = Integer.MAX_VALUE;
            sample(result, "demo_members", labels + ",mode=\"share\"", shareMembers);
            for (TopicPartition partition : partitions) {
                long eligible = group.members().stream().filter(member -> member.assignment().topicPartitions().contains(partition)).count();
                minimumShareEligibility = Math.min(minimumShareEligibility, (int) eligible);
                sample(result, "demo_partition_eligible_members", labels + ",mode=\"share\",partition=\"" + partition.partition() + "\"", eligible);
            }
            Map<TopicPartition, SharePartitionOffsetInfo> offsets = admin.listShareGroupOffsets(Map.of(shareGroup,
                    new ListShareGroupOffsetsSpec().topicPartitions(partitions))).partitionsToOffsetInfo(shareGroup).get(5, TimeUnit.SECONDS);
            Map<Integer, Long> lag = new HashMap<>();
            offsets.forEach((partition, offset) -> {
                if (offset != null) {
                    offset.lag().ifPresent(remaining -> {
                        lag.put(partition.partition(), remaining);
                        sample(result, "demo_source_lag", labels + ",mode=\"share\",partition=\"" + partition.partition() + "\"", remaining);
                    });
                }
            });
            shareLag = Map.copyOf(lag);
            sample(result, "demo_admin_ok", labels + ",mode=\"share\"", 1);
        } catch (InterruptedException | ExecutionException | TimeoutException | RuntimeException e) {
            shareMembers = -1;
            minimumShareEligibility = 0;
            shareLag = Map.of();
            sample(result, "demo_admin_ok", labels + ",mode=\"share\"", 0);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        snapshot = result.toString();
    }

    synchronized boolean ready(int tasks) {
        return classicMembers == tasks && shareMembers == tasks
                && minimumClassicEligibility == 1 && minimumShareEligibility >= (tasks + 2) / 3;
    }

    synchronized boolean completed(Set<Integer> partitions) {
        Map<Integer, Long> classic = classicLag;
        Map<Integer, Long> share = shareLag;
        return partitions.stream().allMatch(partition -> Long.valueOf(0).equals(classic.get(partition))
                && Long.valueOf(0).equals(share.get(partition)));
    }

    String metrics() {
        return snapshot;
    }

    private static void sample(StringBuilder out, String name, String labels, Number value) {
        out.append(name).append('{').append(labels).append("} ").append(value).append('\n');
    }
}
