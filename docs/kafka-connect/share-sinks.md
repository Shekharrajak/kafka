---
title: Share-group sink workers
description: Experimental at-least-once share-group consumption for compatible Kafka Connect sinks.
weight: 25
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

Kafka Connect can opt into share-group consumption for sink tasks that extend
`org.apache.kafka.connect.sink.ShareSinkTask`. Ordinary `SinkTask` implementations
continue to use a partition-assigned consumer by default.

```properties
name=shared-events
connector.class=com.example.DurableSinkConnector
topics=events
tasks.max=9
consumer.type=share
errors.tolerance=none
```

The compatible connector must return a `ShareSinkTask` from `taskClass()`. Its
`put()` must durably complete every supplied record before returning. A buffered
or asynchronous sink must not declare this capability without waiting for all
writes to complete. This is an experimental API/configuration, not a released
compatibility contract.

## Delivery and recovery

The worker creates `KafkaShareConsumer` with explicit acknowledgements, applies
Connect converters and transformations, writes the batch and only then ACCEPTs
its original Kafka records. It checks synchronous acknowledgement results before
polling another batch. A deliberately filtered record is ACCEPTed without a sink
write. Conversion, transformation, sink and acknowledgement failures fail the task;
restart the task to retry unacknowledged deliveries.

Delivery is **at least once**. A sink effect followed by a process failure, expired
record lock or failed acknowledgement may be applied again. Sink transactions
alone do not atomically commit Kafka acknowledgements. Record delivery is not
partition ordered, and a higher completed offset cannot suppress a lower offset.
Configure record-lock duration, retention and delivery-attempt limits for the
workload. This path does not use transactional acknowledgements.

## Configuration and lifecycle

A broker with share groups enabled is required. The default group ID is
`connect-share-<connector-name>`, separate from the conventional consumer-group
ID. Standard worker `consumer.*` settings, connector `consumer.override.*` settings
and override-policy checks apply, including authentication and TLS. Explicitly
configured `auto.offset.reset`, `enable.auto.commit`, implicit acknowledgements
and other unsupported share-client settings are rejected. Configure the starting
position through share-group administration. Switching consumer type does not migrate
consumer-group offsets; choose the share starting position before switching.

`tasks.max` controls task concurrency independently of the topic partition count;
actual work distribution follows the broker's share assignor. Connector pause
finishes an in-flight batch before waiting; resume continues polling. Task
shutdown closes the sink and share consumer. No partition-offset commit is used.

The task context exposes partitions observed by the task, which may also be used
by other tasks. It does not imply exclusive partition ownership. Writers are
opened when a partition is first observed and closed at task shutdown.
`requestCommit()` adds no boundary because every batch is acknowledged immediately.
A retry timeout does not cause local sink retries; task failures require restart.

The initial implementation rejects `topics.regex`, DLQs and error tolerance other
than `none`. Per-partition pause/resume and offset seeking are unsupported. Connect
REST offset inspection/reset/alter operations are rejected for share sinks; use
share-group administration instead. The conventional consumer path is unchanged.
