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
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NativeGroupMetricsTest {
    @Test
    void missingShareLagIsUnknownAndKnownLagComesFromTheAdminResponse() {
        Admin admin = mock(Admin.class);
        DescribeConsumerGroupsResult classicResult = mock(DescribeConsumerGroupsResult.class);
        ConsumerGroupDescription classic = mock(ConsumerGroupDescription.class);
        when(classic.members()).thenReturn(List.of());
        when(classicResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of("classic", classic)));
        when(admin.describeConsumerGroups(List.of("classic"))).thenReturn(classicResult);
        ListConsumerGroupOffsetsResult committed = mock(ListConsumerGroupOffsetsResult.class);
        when(committed.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.listConsumerGroupOffsets("classic")).thenReturn(committed);
        ListOffsetsResult ends = mock(ListOffsetsResult.class);
        when(ends.all()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.listOffsets(anyMap())).thenReturn(ends);
        DescribeShareGroupsResult shareResult = mock(DescribeShareGroupsResult.class);
        ShareGroupDescription share = mock(ShareGroupDescription.class);
        when(share.members()).thenReturn(List.of());
        when(shareResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of("share", share)));
        when(admin.describeShareGroups(List.of("share"))).thenReturn(shareResult);
        ListShareGroupOffsetsResult offsets = mock(ListShareGroupOffsetsResult.class);
        when(admin.listShareGroupOffsets(anyMap())).thenReturn(offsets);
        TopicPartition partition = new TopicPartition("input", 0);
        when(offsets.partitionsToOffsetInfo("share")).thenReturn(KafkaFuture.completedFuture(Map.of(partition,
                new SharePartitionOffsetInfo(100, Optional.empty(), Optional.empty()))));
        NativeGroupMetrics metrics = new NativeGroupMetrics(admin);
        metrics.refresh("test", "input", "classic", "share");
        assertFalse(metrics.metrics().contains("demo_source_lag"));
        when(offsets.partitionsToOffsetInfo("share")).thenReturn(KafkaFuture.completedFuture(Map.of(partition,
                new SharePartitionOffsetInfo(100, Optional.empty(), Optional.of(7L)))));
        metrics.refresh("test", "input", "classic", "share");
        assertTrue(metrics.metrics().contains("demo_source_lag{run_id=\"test\",mode=\"share\",partition=\"0\"} 7"));
    }
}
