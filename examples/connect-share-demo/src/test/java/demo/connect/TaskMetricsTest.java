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

import org.apache.kafka.connect.errors.ConnectException;

import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskMetricsTest {
    @Test
    void knownTaskPublishesIdlePartitionGaugesBeforeItsFirstInput() {
        TaskMetrics metrics = new TaskMetrics("classic", "8", "instance");
        metrics.registerRun("run");
        String text = metrics.render();
        for (int partition = 0; partition < 3; partition++) {
            assertTrue(text.contains("demo_task_processing{mode=\"classic\",task=\"8\",instance=\"instance\",run_id=\"run\",partition=\""
                    + partition + "\"} 0"));
        }
    }

    @Test
    void bindFailureFailsStartupAndDoesNotPreventALaterMetricsServer() throws Exception {
        String previous = System.getProperty("demo.metrics.port");
        int port;
        try {
            try (ServerSocket occupied = new ServerSocket(0)) {
                port = occupied.getLocalPort();
                System.setProperty("demo.metrics.port", Integer.toString(port));
                assertThrows(ConnectException.class, () -> TaskMetrics.open("share", "0", "blocked"));
            }
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            try (TaskMetrics metrics = TaskMetrics.open("share", "0", "running")) {
                metrics.begin("run", 1);
                HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/metrics"))
                        .timeout(Duration.ofSeconds(5)).build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                assertEquals(200, response.statusCode());
                assertTrue(response.body().contains("instance=\"running\""));
                assertTrue(response.body().contains("demo_task_processing"));
            }
        } finally {
            if (previous == null) {
                System.clearProperty("demo.metrics.port");
            } else {
                System.setProperty("demo.metrics.port", previous);
            }
        }
    }

    @Test
    void acceptsConfiguredPortsAndRejectsInvalidMetricsEndpoints() {
        assertEquals(9101, TaskMetrics.port("9101"));
        assertEquals(19102, TaskMetrics.port("19102"));
        for (String invalid : new String[]{"0", "-1", "65536", "not-a-port"}) {
            assertThrows(ConnectException.class, () -> TaskMetrics.port(invalid));
        }
    }

    @Test
    void separatesRunsAndRetiresActiveGaugesWithoutCountingACompletion() {
        TaskMetrics metrics = new TaskMetrics("classic", "1", "instance");
        metrics.begin("first", 0);
        metrics.end("first", 0, true);
        metrics.begin("second", 0);
        metrics.close();
        String text = metrics.render();
        assertTrue(text.contains("demo_task_processed_total{mode=\"classic\",task=\"1\",instance=\"instance\",run_id=\"first\",partition=\"0\"} 1"));
        assertTrue(text.contains("demo_task_processed_total{mode=\"classic\",task=\"1\",instance=\"instance\",run_id=\"second\",partition=\"0\"} 0"));
        assertTrue(text.contains("demo_task_processing{mode=\"classic\",task=\"1\",instance=\"instance\",run_id=\"second\",partition=\"0\"} 0"));
    }
}
