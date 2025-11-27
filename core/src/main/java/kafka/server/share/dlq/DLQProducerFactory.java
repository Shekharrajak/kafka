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
package kafka.server.share.dlq;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Factory for creating DLQ-specific Kafka producer and admin clients.
 *
 * <p>This factory configures producers and admin clients with settings optimized for DLQ operations:</p>
 * <ul>
 *   <li>Reliability: acks=all, retries enabled, idempotence enabled</li>
 *   <li>Performance: Batching enabled, compression configured</li>
 *   <li>Resource management: Bounded memory, appropriate timeouts</li>
 * </ul>
 */
public class DLQProducerFactory {
    private static final Logger log = LoggerFactory.getLogger(DLQProducerFactory.class);

    /**
     * Creates a Kafka producer configured for DLQ writes.
     *
     * <p>Producer configuration:</p>
     * <ul>
     *   <li>acks=all: Ensure durability of DLQ records</li>
     *   <li>retries=Integer.MAX_VALUE: Retry on transient failures</li>
     *   <li>enable.idempotence=true: Prevent duplicates on retries</li>
     *   <li>batch.size=16KB: Batch DLQ records for efficiency</li>
     *   <li>linger.ms=10ms: Small delay to allow batching</li>
     *   <li>buffer.memory=32MB: Bounded memory to prevent OOM</li>
     *   <li>compression.type: Configured from ShareGroupConfig</li>
     * </ul>
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param config Share group configuration
     * @return Configured KafkaProducer for DLQ writes
     */
    public static KafkaProducer<byte[], byte[]> createDLQProducer(
            String bootstrapServers,
            ShareGroupConfig config
    ) {
        Properties props = new Properties();

        // Basic producer configs
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "share-group-dlq-producer");

        // Reliability configs - ensure DLQ records are durably written
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // Performance configs - batch DLQ records for efficiency
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // 16KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10"); // 10ms linger for batching
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"); // 32MB buffer

        // Compression - use configured compression type
        String compressionType = config.shareGroupDlqCompressionType();
        if (!compressionType.equalsIgnoreCase("producer")) {
            // If not "producer", use the specific compression type
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        } else {
            // "producer" means no compression by DLQ producer (preserve original)
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        }

        // Timeout configs
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000"); // 30 seconds
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); // 2 minutes

        // Partitioner - use default partitioner
        // Records with same key go to same partition, maintaining per-key ordering

        log.info("Creating DLQ producer with compression={}, batch.size=16KB, linger.ms=10ms",
                compressionType);

        return new KafkaProducer<>(props);
    }

    /**
     * Creates a Kafka admin client configured for DLQ topic management.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @return Configured Admin client
     */
    public static Admin createDLQAdminClient(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();

        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "share-group-dlq-admin");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        log.info("Creating DLQ admin client for bootstrap servers: {}", bootstrapServers);

        return Admin.create(props);
    }

    /**
     * Creates a complete DLQ infrastructure (admin client, topic manager, writer).
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param config Share group configuration
     * @param brokerId Broker ID for metadata headers
     * @return DLQWriter instance ready for use
     */
    public static DLQWriter createDLQInfrastructure(
            String bootstrapServers,
            ShareGroupConfig config,
            int brokerId
    ) {
        if (!config.shareGroupDlqEnabled()) {
            log.info("DLQ is disabled, skipping DLQ infrastructure creation");
            return null;
        }

        log.info("Creating DLQ infrastructure for broker {}", brokerId);

        // Create admin client for topic management
        Admin adminClient = createDLQAdminClient(bootstrapServers);

        // Create topic manager
        DLQTopicManager topicManager = new DLQTopicManager(adminClient, config);

        // Create producer for DLQ writes
        KafkaProducer<byte[], byte[]> dlqProducer = createDLQProducer(bootstrapServers, config);

        // Create DLQ writer
        DLQWriter dlqWriter = new DLQWriter(dlqProducer, config, topicManager, brokerId);

        log.info("DLQ infrastructure created successfully for broker {}", brokerId);

        return dlqWriter;
    }
}
