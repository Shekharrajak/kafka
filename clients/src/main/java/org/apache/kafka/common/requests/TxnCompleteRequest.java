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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.TxnCompleteRequestData;
import org.apache.kafka.common.message.TxnCompleteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class TxnCompleteRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<TxnCompleteRequest> {
        private final TxnCompleteRequestData data;

        public Builder(TxnCompleteRequestData data) {
            this(data, false);
        }

        public Builder(TxnCompleteRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.TXN_COMPLETE, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public TxnCompleteRequest build(short version) {
            return new TxnCompleteRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final TxnCompleteRequestData data;

    public TxnCompleteRequest(TxnCompleteRequestData data, short version) {
        super(ApiKeys.TXN_COMPLETE, version);
        this.data = data;
    }

    @Override
    public TxnCompleteRequestData data() {
        return data;
    }

    @Override
    public TxnCompleteResponse getErrorResponse(int throttleTimeMs, Throwable exception) {
        Errors error = Errors.forException(exception);
        TxnCompleteResponseData response = new TxnCompleteResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(error.message());
        data.transactions().forEach(transaction -> response.transactions().add(
            new TxnCompleteResponseData.TxnCompleteTransactionResponse()
                .setTransactionalId(transaction.transactionalId())
                .setProducerId(transaction.producerId())
                .setProducerEpoch(transaction.producerEpoch())
                .setErrorCode(error.code())
                .setErrorMessage(error.message())));
        return new TxnCompleteResponse(response);
    }

    public static TxnCompleteRequest parse(Readable readable, short version) {
        return new TxnCompleteRequest(new TxnCompleteRequestData(readable, version), version);
    }
}
