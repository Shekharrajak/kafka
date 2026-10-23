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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ShareConsumerSettings {
    static Map<String, Object> create(String connectorName, Map<String, Object> base,
                                      Map<String, Object> workerOverrides, Map<String, Object> connectorOverrides) {
        Map<String, Object> result = new HashMap<>(base);
        for (String key : List.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            if (workerOverrides.containsKey(key) || connectorOverrides.containsKey(key)) {
                throw new ConnectException(key + " cannot be set for share consumption");
            }
            result.remove(key);
        }
        String acknowledgementMode = ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG;
        if (!"explicit".equals(result.getOrDefault(acknowledgementMode, "explicit"))) {
            throw new ConnectException("Share sink consumption requires explicit acknowledgements");
        }
        result.put(acknowledgementMode, "explicit");
        if (!workerOverrides.containsKey(ConsumerConfig.GROUP_ID_CONFIG)
                && !connectorOverrides.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            result.put(ConsumerConfig.GROUP_ID_CONFIG, "connect-share-" + connectorName);
        }
        return result;
    }
}
