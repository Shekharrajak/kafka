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

import org.apache.kafka.common.message.TxnShareAcknowledgeRequestData;
import org.apache.kafka.common.message.TxnShareAcknowledgeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class TxnShareAcknowledgeRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<TxnShareAcknowledgeRequest> {
        private final TxnShareAcknowledgeRequestData data;

        public Builder(TxnShareAcknowledgeRequestData data) {
            this(data, false);
        }

        public Builder(TxnShareAcknowledgeRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.TXN_SHARE_ACKNOWLEDGE, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public TxnShareAcknowledgeRequest build(short version) {
            return new TxnShareAcknowledgeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final TxnShareAcknowledgeRequestData data;

    public TxnShareAcknowledgeRequest(TxnShareAcknowledgeRequestData data, short version) {
        super(ApiKeys.TXN_SHARE_ACKNOWLEDGE, version);
        this.data = data;
    }

    @Override
    public TxnShareAcknowledgeRequestData data() {
        return data;
    }

    @Override
    public TxnShareAcknowledgeResponse getErrorResponse(int throttleTimeMs, Throwable exception) {
        Errors error = Errors.forException(exception);
        TxnShareAcknowledgeResponseData response = new TxnShareAcknowledgeResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setErrorMessage(error.message());
        data.topics().forEach(topic -> {
            TxnShareAcknowledgeResponseData.TxnShareAcknowledgeTopicResponse result =
                new TxnShareAcknowledgeResponseData.TxnShareAcknowledgeTopicResponse().setTopicId(topic.topicId());
            topic.partitions().forEach(partition -> result.partitions().add(
                new TxnShareAcknowledgeResponseData.TxnShareAcknowledgePartitionResponse()
                    .setPartitionIndex(partition.partitionIndex())
                    .setErrorCode(error.code())
                    .setErrorMessage(error.message())));
            response.responses().add(result);
        });
        return new TxnShareAcknowledgeResponse(response);
    }

    public static TxnShareAcknowledgeRequest parse(Readable readable, short version) {
        return new TxnShareAcknowledgeRequest(new TxnShareAcknowledgeRequestData(readable, version), version);
    }
}
