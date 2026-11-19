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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

public final class DemoControl implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private final String bootstrap;
    private final Map<String, String> connect;
    private final Path dataDir;
    private final Admin admin;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT).build();
    private final ExecutorService jobs = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService requests = Executors.newFixedThreadPool(4);
    private final Map<String, Map<String, Object>> reports = new LinkedHashMap<>();
    private final HttpServer server;
    private volatile Run current;
    private volatile boolean requiresInspection;

    DemoControl(String bootstrap, String classicConnect, String shareConnect, Path dataDir, int port) throws IOException {
        this.bootstrap = bootstrap;
        connect = Map.of("classic", classicConnect, "share", shareConnect);
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        reports.putAll(storedReports(dataDir));
        requiresInspection = reports.values().stream().anyMatch(report -> Boolean.TRUE.equals(report.get("requiresInspection")));
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", "10000", "request.timeout.ms", "5000"));
        server = HttpServer.create(new InetSocketAddress(port), 32);
        server.createContext("/", this::handle);
        server.setExecutor(requests);
        monitor.scheduleWithFixedDelay(this::refresh, 0, 5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException {
        DemoControl control = new DemoControl(env("BOOTSTRAP_SERVERS", "broker:9092"),
                env("CLASSIC_CONNECT", "http://classic-1:8083"), env("SHARE_CONNECT", "http://share-1:8083"),
                Path.of(env("DATA_DIR", "/data")), Integer.parseInt(env("PORT", "8090")));
        Runtime.getRuntime().addShutdownHook(new Thread(control::close, "demo-control-shutdown"));
        control.server.start();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            if ("GET".equals(exchange.getRequestMethod())) {
                if ("/health".equals(path)) {
                    json(exchange, 200, Map.of("status", "UP"));
                } else if ("/state".equals(path)) {
                    json(exchange, 200, state());
                } else if ("/metrics".equals(path)) {
                    byte[] body = metrics().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                } else if (path.startsWith("/runs/")) {
                    Map<String, Object> report = report(path.substring("/runs/".length()));
                    json(exchange, report == null ? 404 : 200, report == null ? Map.of("error", "Unknown run") : report);
                } else {
                    json(exchange, 404, Map.of("error", "Unknown endpoint"));
                }
            } else if ("POST".equals(exchange.getRequestMethod()) && "/runs".equals(path)) {
                byte[] body = exchange.getRequestBody().readNBytes(65_537);
                if (body.length > 65_536) {
                    json(exchange, 413, Map.of("error", "Request is too large"));
                    return;
                }
                Request request = Request.parse(JSON.readTree(body));
                json(exchange, 202, start(request));
            } else {
                json(exchange, 405, Map.of("error", "Unsupported method"));
            }
        } catch (IllegalArgumentException e) {
            json(exchange, 400, Map.of("error", e.getMessage()));
        } catch (IllegalStateException e) {
            json(exchange, 409, Map.of("error", e.getMessage()));
        } catch (Exception e) {
            json(exchange, 500, Map.of("error", e.toString()));
        } finally {
            exchange.close();
        }
    }

    synchronized Map<String, Object> start(Request request) throws IOException {
        if (requiresInspection) {
            throw new IllegalStateException("An unresolved or failed run requires inspection; preserve its evidence and stop its connectors before using a new data directory");
        }
        if (current != null && !current.terminal()) {
            throw new IllegalStateException("A run is already active");
        }
        if (reports.containsKey(request.runId()) || Files.exists(dataDir.resolve(request.runId() + ".request.json"))) {
            throw new IllegalStateException("Run ID already reserved; inspect its existing outcome");
        }
        if (reports.size() >= 10) {
            throw new IllegalStateException("This controller session already contains ten runs; preserve reports before using a new data directory");
        }
        durableWrite(dataDir.resolve(request.runId() + ".request.json"), JSON.writeValueAsBytes(request), true);
        Run run = new Run(request);
        current = run;
        persist(run);
        jobs.submit(() -> execute(run));
        return run.report();
    }

    private void execute(Run run) {
        AtomicBoolean observing = new AtomicBoolean(true);
        AtomicReference<Throwable> observerFailure = new AtomicReference<>();
        Thread observer = null;
        try {
            prepare(run);
            CountDownLatch observerReady = new CountDownLatch(1);
            observer = new Thread(() -> observe(run, observing, observerFailure, observerReady), "demo-output-observer");
            observer.setDaemon(true);
            observer.start();
            if (!observerReady.await(20, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Output observer did not become ready");
            }
            checkObserver(observerFailure);
            run.startedAtMs = System.currentTimeMillis();
            run.ledger.start(run.startedAtMs);
            run.status = "GENERATING";
            persist(run);
            publish(run, observerFailure);
            run.generationFinishedAtMs = System.currentTimeMillis();
            run.ledger.generationFinished(run.generationFinishedAtMs);
            run.status = "DRAINING";
            persist(run);
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
            while (!run.ledger.complete()) {
                checkObserver(observerFailure);
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Timed out draining all published input identities");
                }
                Thread.sleep(100);
            }
            if (!run.ledger.valid()) {
                throw new IllegalStateException("Output validation found invalid or unexpected records");
            }
            confirmSourceProgress(run);
            stopConnectors(run);
            observing.set(false);
            observer.join(15_000);
            if (observer.isAlive()) {
                throw new IllegalStateException("Output observer failed to stop");
            }
            checkObserver(observerFailure);
            run.status = "AUDITING";
            persist(run);
            run.audit = audit(run);
            run.status = "FINISHED";
        } catch (Exception e) {
            run.error = e.toString();
            run.status = "FAILED";
            requiresInspection = true;
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            observing.set(false);
            if (observer != null) {
                observer.interrupt();
            }
            run.finishedAtMs = System.currentTimeMillis();
            try {
                persist(run);
            } catch (IOException e) {
                run.status = "FAILED";
                run.error = "Could not persist final report: " + e;
                requiresInspection = true;
                System.err.println(run.error);
            }
        }
    }

    private void prepare(Run run) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(run.ledger.inputTopic(), 3, (short) 1));
        run.outputs.values().forEach(topic -> topics.add(new NewTopic(topic, 3, (short) 1)));
        admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        ConfigResource group = new ConfigResource(ConfigResource.Type.GROUP, run.groups.get("share"));
        admin.incrementalAlterConfigs(Map.of(group, List.of(
                new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("share.record.lock.duration.ms", "3000"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("share.renew.acknowledge.enable", "true"), AlterConfigOp.OpType.SET)
        ))).all().get(20, TimeUnit.SECONDS);
        for (String mode : RunLedger.MODES) {
            Map<String, String> config = connectorConfig(run, mode);
            connectRequest(mode, "POST", "/connectors", Map.of("name", connectorName(run, mode), "config", config), 201);
        }
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while (System.nanoTime() < deadline) {
            boolean ready = true;
            for (String mode : RunLedger.MODES) {
                JsonNode status;
                try {
                    status = connectRequest(mode, "GET", "/connectors/" + connectorName(run, mode) + "/status", null, 200);
                } catch (IOException unavailable) {
                    ready = false;
                    continue;
                }
                if ("FAILED".equals(status.path("connector").path("state").asText())) {
                    throw new IllegalStateException(mode + " connector failed: " + status);
                }
                ready &= "RUNNING".equals(status.path("connector").path("state").asText());
                if (status.path("tasks").size() != run.request.tasks()) {
                    ready = false;
                }
                for (JsonNode task : status.path("tasks")) {
                    if ("FAILED".equals(task.path("state").asText())) {
                        throw new IllegalStateException(mode + " task failed: " + task);
                    }
                    ready &= "RUNNING".equals(task.path("state").asText());
                }
            }
            run.nativeMetrics.refresh(run.request.runId(), run.ledger.inputTopic(), run.groups.get("classic"), run.groups.get("share"));
            if (ready && run.nativeMetrics.ready(run.request.tasks())) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Connect tasks and native consumer groups did not become ready");
    }

    private Map<String, String> connectorConfig(Run run, String mode) {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("connector.class", "demo.connect." + (mode.equals("share") ? "ShareDemoSinkConnector" : "DemoSinkConnector"));
        config.put("tasks.max", Integer.toString(run.request.tasks()));
        config.put("topics", run.ledger.inputTopic());
        config.put("consumer.type", mode.equals("share") ? "share" : "consumer");
        if (mode.equals("classic")) {
            config.put("consumer.override.auto.offset.reset", "earliest");
        }
        config.put("consumer.override.max.poll.records", "5");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("errors.tolerance", "none");
        config.put("demo.mode", mode);
        config.put("demo.run.id", run.request.runId());
        config.put("demo.bootstrap.servers", bootstrap);
        config.put("demo.output.topic", run.outputs.get(mode));
        if (mode.equals("share")) {
            config.put("share.group.id", run.groups.get(mode));
            config.put("share.processing.max.age.ms", "60000");
            config.put("share.processing.cancel.grace.ms", "5000");
        }
        return config;
    }

    private void publish(Run run, AtomicReference<Throwable> observerFailure) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Semaphore pending = new Semaphore(1024);
        long started = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int id = 0; id < run.request.records(); id++) {
                long scheduled = started + (long) (id * 1_000_000_000.0 / run.request.rate());
                while (System.nanoTime() < scheduled) {
                    LockSupport.parkNanos(Math.min(scheduled - System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(20)));
                    if (Thread.interrupted()) {
                        throw new InterruptedException("Generation interrupted");
                    }
                }
                checkObserver(observerFailure);
                checkObserver(failure);
                if (!pending.tryAcquire(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Producer is not keeping up with the offered load");
                }
                run.maxPublisherLatenessMs = Math.max(run.maxPublisherLatenessMs,
                        TimeUnit.NANOSECONDS.toMillis(Math.max(0, System.nanoTime() - scheduled)));
                int partition = run.request.partition(id);
                RunLedger.Job job = new RunLedger.Job(run.request.runId(), id, "customer-" + partition,
                        System.currentTimeMillis(), run.request.work(id), "document-" + run.request.runId() + "-" + id);
                run.ledger.register(job, partition);
                producer.send(new ProducerRecord<>(run.ledger.inputTopic(), partition, Integer.toString(id), JSON.writeValueAsString(job)),
                        (metadata, error) -> {
                            try {
                                if (error != null) {
                                    failure.compareAndSet(null, error);
                                } else {
                                    run.ledger.published(job.jobId(), metadata.partition(), metadata.offset());
                                }
                            } catch (RuntimeException e) {
                                failure.compareAndSet(null, e);
                            } finally {
                                pending.release();
                            }
                        });
            }
            producer.flush();
            checkObserver(failure);
        }
    }

    private void observe(Run run, AtomicBoolean active, AtomicReference<Throwable> failure, CountDownLatch ready) {
        try (KafkaConsumer<String, String> consumer = consumer()) {
            List<TopicPartition> partitions = outputPartitions(run);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            ready.countDown();
            while (active.get()) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    String mode = record.topic().equals(run.outputs.get("classic")) ? "classic" : "share";
                    run.ledger.observe(mode, record.partition(), record.offset(), record.value(), System.currentTimeMillis());
                }
            }
        } catch (Exception e) {
            if (active.get()) {
                failure.compareAndSet(null, e);
            }
        } finally {
            ready.countDown();
        }
    }

    private Map<String, Object> audit(Run run) throws Exception {
        RunLedger audit = run.ledger.auditCopy();
        Map<String, Object> result = new LinkedHashMap<>();
        try (KafkaConsumer<String, String> consumer = consumer()) {
            List<TopicPartition> partitions = outputPartitions(run);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (true) {
                boolean finished = true;
                for (TopicPartition partition : partitions) {
                    finished &= consumer.position(partition) >= ends.get(partition);
                }
                if (finished) {
                    break;
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Physical output audit timed out");
                }
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    if (record.offset() < ends.get(new TopicPartition(record.topic(), record.partition()))) {
                        String mode = record.topic().equals(run.outputs.get("classic")) ? "classic" : "share";
                        audit.observe(mode, record.partition(), record.offset(), record.value(), System.currentTimeMillis());
                    }
                }
            }
            result.putAll(audit.auditSummaries());
            result.put("capturedEndOffsets", ends.entrySet().stream().collect(LinkedHashMap::new,
                    (map, entry) -> map.put(entry.getKey().toString(), entry.getValue()), Map::putAll));
        }
        result.put("passed", audit.valid());
        run.audit = result;
        if (!audit.valid()) {
            throw new IllegalStateException("Independent physical audit found missing or invalid results");
        }
        return result;
    }

    private void stopConnectors(Run run) throws Exception {
        for (String mode : RunLedger.MODES) {
            connectRequest(mode, "PUT", "/connectors/" + connectorName(run, mode) + "/stop", null, 204, 202);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            boolean stopped = true;
            for (String mode : RunLedger.MODES) {
                JsonNode status = connectRequest(mode, "GET", "/connectors/" + connectorName(run, mode) + "/status", null, 200);
                stopped &= "STOPPED".equals(status.path("connector").path("state").asText()) && status.path("tasks").isEmpty();
            }
            if (stopped) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("Connect tasks did not stop before the bounded physical audit");
    }

    private void confirmSourceProgress(Run run) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        String lastUnavailable = "";
        while (System.nanoTime() < deadline) {
            try {
                if (tasksRunning(run)) {
                    run.nativeMetrics.refresh(run.request.runId(), run.ledger.inputTopic(), run.groups.get("classic"), run.groups.get("share"));
                    if (run.nativeMetrics.completed(run.ledger.publishedPartitions())) {
                        run.sourceProgressConfirmed = true;
                        return;
                    }
                }
            } catch (IOException e) {
                lastUnavailable = e.toString();
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Outputs completed but healthy tasks and fresh native input progress were not fully confirmed: " + lastUnavailable);
    }

    private boolean tasksRunning(Run run) throws Exception {
        for (String mode : RunLedger.MODES) {
            JsonNode status = connectRequest(mode, "GET", "/connectors/" + connectorName(run, mode) + "/status", null, 200);
            if (status.path("tasks").size() != run.request.tasks()) {
                return false;
            }
            for (JsonNode task : status.path("tasks")) {
                if (!"RUNNING".equals(task.path("state").asText())) {
                    return false;
                }
            }
        }
        return true;
    }

    private KafkaConsumer<String, String> consumer() {
        return new KafkaConsumer<>(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-verifier-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000"), new StringDeserializer(), new StringDeserializer());
    }

    private JsonNode connectRequest(String mode, String method, String path, Object body, int... accepted) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(connect.get(mode) + path)).timeout(REQUEST_TIMEOUT);
        builder.header("Content-Type", "application/json");
        builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)));
        HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        for (int status : accepted) {
            if (response.statusCode() == status) {
                return response.body().isBlank() ? JSON.createObjectNode() : JSON.readTree(response.body());
            }
        }
        throw new IOException(mode + " Connect " + method + " " + path + " returned " + response.statusCode() + ": " + response.body());
    }

    private void refresh() {
        Run run = current;
        if (run != null && !run.terminal()) {
            try {
                run.nativeMetrics.refresh(run.request.runId(), run.ledger.inputTopic(), run.groups.get("classic"), run.groups.get("share"));
            } catch (RuntimeException e) {
                System.err.println("Native group metric refresh failed: " + e);
            }
        }
    }

    private synchronized Map<String, Object> state() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("current", current == null ? null : current.report());
        result.put("runs", new ArrayList<>(reports.values()));
        result.put("requiresInspection", requiresInspection);
        return result;
    }

    private synchronized Map<String, Object> report(String id) {
        return current != null && current.request.runId().equals(id) ? current.report() : reports.get(id);
    }

    private String metrics() {
        Run run = current;
        if (run == null) {
            return "demo_control_ready 1\n";
        }
        String labels = "run_id=\"" + run.request.runId() + "\"";
        return "demo_control_ready 1\n"
                + "demo_run_info{" + labels + ",scenario=\"" + run.request.scenario() + "\",tasks=\"" + run.request.tasks() + "\",share_policy=\"" + env("SHARE_POLICY", "simple") + "\"} 1\n"
                + "demo_run_status{" + labels + ",status=\"" + run.status + "\"} 1\n"
                + "demo_requested_rate{" + labels + "} " + run.request.rate() + "\n"
                + run.ledger.metrics(System.currentTimeMillis()) + run.nativeMetrics.metrics();
    }

    private synchronized void persist(Run run) throws IOException {
        Map<String, Object> report = run.report();
        Path target = dataDir.resolve(run.request.runId() + ".report.json");
        Path pending = dataDir.resolve(run.request.runId() + ".report.tmp");
        durableWrite(pending, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(report), false);
        Files.move(pending, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        reports.put(run.request.runId(), report);
    }

    static Map<String, Map<String, Object>> storedReports(Path dataDir) throws IOException {
        Map<String, Map<String, Object>> loaded = new LinkedHashMap<>();
        try (var files = Files.list(dataDir)) {
            for (Path path : files.filter(file -> fileName(file).endsWith(".request.json")).sorted().toList()) {
                String id = fileName(path).replace(".request.json", "");
                Path reportPath = dataDir.resolve(id + ".report.json");
                Map<String, Object> report = Files.exists(reportPath)
                        ? JSON.readValue(Files.readString(reportPath), new TypeReference<>() { })
                        : new LinkedHashMap<>(Map.of("runId", id));
                if (!"FINISHED".equals(report.get("status")) && !"FAILED".equals(report.get("status"))) {
                    report.put("status", "FAILED");
                    report.put("error", "Controller restarted with an unresolved run; inspect retained topics and workers before proceeding");
                    report.put("requiresInspection", true);
                }
                loaded.put(id, report);
            }
        }
        return loaded;
    }

    private static String fileName(Path path) {
        Path name = path.getFileName();
        return name == null ? "" : name.toString();
    }

    private static void durableWrite(Path target, byte[] body, boolean createNew) throws IOException {
        StandardOpenOption option = createNew ? StandardOpenOption.CREATE_NEW : StandardOpenOption.CREATE;
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.WRITE, option, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.wrap(body);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    private static List<TopicPartition> outputPartitions(Run run) {
        return run.outputs.values().stream().flatMap(topic -> IntStream.range(0, 3).mapToObj(partition -> new TopicPartition(topic, partition))).toList();
    }

    private static String connectorName(Run run, String mode) {
        return "demo-" + mode + "-" + run.request.runId();
    }

    private static void checkObserver(AtomicReference<Throwable> failure) {
        Throwable error = failure.get();
        if (error != null) {
            throw new IllegalStateException("Run operation failed; publication or completion may be unknown", error);
        }
    }

    private static void json(HttpExchange exchange, int status, Object value) throws IOException {
        byte[] body = JSON.writeValueAsBytes(value);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    private static String env(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, defaultValue);
    }

    @Override
    public void close() {
        server.stop(1);
        jobs.shutdownNow();
        monitor.shutdownNow();
        requests.shutdownNow();
        admin.close(Duration.ofSeconds(5));
    }

    record Request(String runId, String scenario, int records, double rate, int workMs, int tasks) {
        static Request parse(JsonNode value) {
            Request request = new Request(value.path("runId").asText(), value.path("scenario").asText("balanced"),
                    integer(value, "records", 5_000), value.path("rate").asDouble(120),
                    integer(value, "workMs", 20), integer(value, "tasks", 9));
            if (!request.validIdentity() || !request.validCounts() || !request.validRate(value) || !request.validWork()) {
                throw new IllegalArgumentException("Invalid run: use a unique lowercase runId, a supported scenario, 1..50000 records, 0<rate<=10000, 0..30000 workMs and 1..9 tasks");
            }
            return request;
        }

        private boolean validIdentity() {
            return runId.matches("[a-z0-9][a-z0-9-]{0,39}")
                    && List.of("balanced", "skewed", "mixed", "slow").contains(scenario);
        }

        private boolean validCounts() {
            return records >= 1 && records <= 50_000 && tasks >= 1 && tasks <= 9;
        }

        private boolean validRate(JsonNode value) {
            return (!value.has("rate") || value.path("rate").isNumber())
                    && Double.isFinite(rate) && rate > 0 && rate <= 10_000;
        }

        private boolean validWork() {
            return workMs >= 0 && workMs <= 30_000;
        }

        private static int integer(JsonNode value, String field, int defaultValue) {
            if (!value.has(field)) {
                return defaultValue;
            }
            JsonNode node = value.path(field);
            if (!node.isIntegralNumber() || !node.canConvertToInt()) {
                throw new IllegalArgumentException(field + " must be an integer");
            }
            return node.intValue();
        }

        int partition(int id) {
            return scenario.equals("skewed") ? (id % 20 < 18 ? 0 : id % 20 - 17) : id % 3;
        }

        int work(int id) {
            if (scenario.equals("slow") && id == 0) {
                return 10_000;
            }
            return scenario.equals("mixed") && id % 20 == 0 ? Math.min(30_000, workMs * 10) : workMs;
        }
    }

    private final class Run {
        private final Request request;
        private final RunLedger ledger;
        private final NativeGroupMetrics nativeMetrics = new NativeGroupMetrics(admin);
        private final Map<String, String> outputs;
        private final Map<String, String> groups;
        private volatile String status = "PREPARING";
        private volatile String error;
        private volatile long startedAtMs;
        private volatile long generationFinishedAtMs;
        private volatile long finishedAtMs;
        private volatile long maxPublisherLatenessMs;
        private volatile boolean sourceProgressConfirmed;
        private volatile Map<String, Object> audit = Map.of();

        private Run(Request request) {
            this.request = request;
            ledger = new RunLedger(request.runId(), request.records());
            outputs = Map.of("classic", "demo-classic-" + request.runId(), "share", "demo-share-" + request.runId());
            groups = Map.of("classic", "connect-demo-classic-" + request.runId(), "share", "demo-share-" + request.runId());
        }

        private boolean terminal() {
            return status.equals("FINISHED") || status.equals("FAILED");
        }

        private Map<String, Object> report() {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("runId", request.runId());
            result.put("scenario", request.scenario());
            result.put("status", status);
            result.put("records", request.records());
            result.put("rate", request.rate());
            result.put("workMs", request.workMs());
            result.put("tasks", request.tasks());
            result.put("sharePolicy", env("SHARE_POLICY", "simple"));
            result.put("startedAtMs", startedAtMs);
            result.put("generationFinishedAtMs", generationFinishedAtMs);
            result.put("finishedAtMs", finishedAtMs);
            result.put("measurementFinishedAtMs", ledger.measurementFinishedAtMs());
            result.put("sourceProgressConfirmed", sourceProgressConfirmed);
            result.put("maxPublisherLatenessMs", maxPublisherLatenessMs);
            result.put("actualPublishedPerSecond", generationFinishedAtMs > startedAtMs && startedAtMs > 0
                    ? 1000.0 * ((Number) ledger.summary("classic", generationFinishedAtMs).get("published")).longValue()
                    / (generationFinishedAtMs - startedAtMs) : 0);
            result.put("inputTopic", ledger.inputTopic());
            result.put("outputs", outputs);
            result.put("groups", groups);
            result.put("modes", ledger.summaries(System.currentTimeMillis()));
            result.put("audit", audit);
            result.put("concurrencyClock", "Processing span overlap assumes the shared Docker VM clock");
            result.put("requiresInspection", status.equals("FAILED"));
            if (error != null) {
                result.put("error", error);
            }
            return result;
        }
    }
}
