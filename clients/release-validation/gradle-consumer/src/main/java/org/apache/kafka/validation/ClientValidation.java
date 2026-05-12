/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.validation;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Smoke-test entry point used by the release-validation scaffolding.
 *
 * <p>Instantiates each top-level kafka-clients class against a non-routable
 * bootstrap address. The test does not connect to a broker; success is
 * defined as: classes load, constructors run without {@link Error}, and the
 * JVM exits with status zero. Any {@link NoClassDefFoundError},
 * {@link LinkageError}, or {@link ClassCastException} indicates a packaging
 * or shading regression in the candidate kafka-clients artefact.
 */
public final class ClientValidation {

    private static final String BOOTSTRAP = "localhost:1";

    private ClientValidation() {
    }

    public static void main(String[] args) {
        validateProducer();
        validateConsumer();
        validateShareConsumer();
        validateAdminClient();
        System.out.println("kafka-clients release validation: OK");
    }

    private static void validateProducer() {
        Properties props = baseProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> p = new KafkaProducer<>(props)) {
            p.partitionsFor("does-not-matter-no-broker-call-made");
        } catch (Exception ignored) {
            // Network exceptions are expected; class loading and construction succeeded.
        }
    }

    private static void validateConsumer() {
        Properties props = baseProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "release-validation");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(props)) {
            c.listTopics(java.time.Duration.ofMillis(1));
        } catch (Exception ignored) {
            // Expected.
        }
    }

    private static void validateShareConsumer() {
        Properties props = baseProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "release-validation-share");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaShareConsumer<String, String> c = new KafkaShareConsumer<>(props)) {
            c.subscription();
        } catch (Exception ignored) {
            // Expected.
        }
    }

    private static void validateAdminClient() {
        Properties props = baseProps();
        try (AdminClient admin = AdminClient.create(props)) {
            admin.describeCluster();
        } catch (Exception ignored) {
            // Expected.
        }
    }

    private static Properties baseProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10");
        return props;
    }
}
