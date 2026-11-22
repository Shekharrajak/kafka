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
package org.apache.kafka.connect.sink;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.annotation.InterfaceAudience;

import java.util.Collection;

/**
 * A stateless share sink whose Kafka output and input acknowledgements are committed by the worker.
 * Tasks must produce every output through the supplied handle during the call and perform no external writes.
 */
@InterfaceAudience.Public
public abstract class TransactionalShareSinkTask extends SinkTask {

    @Override
    public final void put(Collection<SinkRecord> records) {
        throw new UnsupportedOperationException("Transactional share sinks require worker-owned output");
    }

    /**
     * Maps or filters this poll unit using a synchronous, task-thread-only output handle.
     *
     * @param records records after conversion and transformations
     * @param output the output handle, valid only during this call
     */
    public abstract void put(Collection<SinkRecord> records, Output output);

    /**
     * Output submitted to the worker-owned Kafka transaction.
     */
    @FunctionalInterface
    public interface Output {
        /**
         * Submits a record for delivery before the worker stages any input acknowledgements.
         *
         * @param record the Kafka output record
         */
        void send(ProducerRecord<byte[], byte[]> record);
    }
}
