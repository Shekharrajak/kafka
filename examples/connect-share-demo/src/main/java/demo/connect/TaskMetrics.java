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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

final class TaskMetrics implements AutoCloseable {
    private static final Map<String, TaskMetrics> REGISTRY = new HashMap<>();
    private static HttpServer server;
    private static ExecutorService executor;

    private final String labels;
    private final String instance;
    private final Map<Input, Counts> inputs = new ConcurrentHashMap<>();

    static synchronized TaskMetrics open(String mode, String task, String instance) {
        int port = port(System.getProperty("demo.metrics.port",
                System.getenv().getOrDefault("DEMO_METRICS_PORT", "9101")));
        if (server == null) {
            try {
                HttpServer listener = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 16);
                ExecutorService workers = Executors.newFixedThreadPool(2, runnable -> {
                    Thread thread = new Thread(runnable, "demo-task-metrics");
                    thread.setDaemon(true);
                    return thread;
                });
                listener.setExecutor(workers);
                listener.createContext("/metrics", TaskMetrics::respond);
                listener.start();
                server = listener;
                executor = workers;
            } catch (IOException e) {
                throw new ConnectException("Cannot bind demo task metrics on port " + port, e);
            }
        }
        TaskMetrics metrics = new TaskMetrics(mode, task, instance);
        REGISTRY.put(instance, metrics);
        return metrics;
    }

    static int port(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port > 0 && port <= 65535) {
                return port;
            }
        } catch (NumberFormatException e) {
            throw new ConnectException("Invalid demo metrics port: " + value, e);
        }
        throw new ConnectException("Invalid demo metrics port: " + value);
    }

    TaskMetrics(String mode, String task, String instance) {
        this.instance = instance;
        labels = "mode=\"" + escape(mode) + "\",task=\"" + escape(task)
                + "\",instance=\"" + escape(instance) + "\"";
    }

    void registerRun(String runId) {
        for (int partition = 0; partition < 3; partition++) {
            counts(runId, partition);
        }
    }

    void begin(String runId, int partition) {
        counts(runId, partition).processing.incrementAndGet();
    }

    void end(String runId, int partition, boolean succeeded) {
        Counts counts = counts(runId, partition);
        counts.processing.decrementAndGet();
        if (succeeded) {
            counts.processed.increment();
        } else {
            counts.failures.increment();
        }
    }

    String render() {
        StringBuilder result = new StringBuilder();
        inputs.forEach((input, counts) -> {
            String tags = "{" + labels + ",run_id=\"" + escape(input.runId)
                    + "\",partition=\"" + input.partition + "\"} ";
            result.append("demo_task_processing").append(tags).append(counts.processing.get()).append('\n');
            result.append("demo_task_processed_total").append(tags).append(counts.processed.sum()).append('\n');
            result.append("demo_task_failures_total").append(tags).append(counts.failures.sum()).append('\n');
        });
        return result.toString();
    }

    private Counts counts(String runId, int partition) {
        return inputs.computeIfAbsent(new Input(runId, partition), ignored -> new Counts());
    }

    private static void respond(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod()) || !"/metrics".equals(exchange.getRequestURI().getPath())) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
        }
        byte[] body;
        synchronized (TaskMetrics.class) {
            StringBuilder result = new StringBuilder();
            REGISTRY.values().forEach(metrics -> result.append(metrics.render()));
            body = result.toString().getBytes(StandardCharsets.UTF_8);
        }
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (var output = exchange.getResponseBody()) {
            output.write(body);
        } finally {
            exchange.close();
        }
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    @Override
    public void close() {
        inputs.values().forEach(counts -> counts.processing.set(0));
        synchronized (TaskMetrics.class) {
            REGISTRY.remove(instance, this);
            if (REGISTRY.isEmpty() && server != null) {
                server.stop(0);
                executor.shutdownNow();
                server = null;
                executor = null;
            }
        }
    }

    private record Input(String runId, int partition) {
    }

    private static final class Counts {
        private final AtomicInteger processing = new AtomicInteger();
        private final LongAdder processed = new LongAdder();
        private final LongAdder failures = new LongAdder();
    }
}
