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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class RunLedger {
    static final List<String> MODES = List.of("classic", "share");
    private static final double[] BUCKETS = {0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300};
    private static final ObjectMapper JSON = new ObjectMapper();
    private final String runId;
    private final Expected[] expected;
    private final BitSet published;
    private final Map<String, Mode> modes = new LinkedHashMap<>();
    private long startedAtMs;
    private long generationFinishedAtMs;

    RunLedger(String runId, int count) {
        this.runId = runId;
        expected = new Expected[count];
        published = new BitSet(count);
        MODES.forEach(mode -> modes.put(mode, new Mode()));
    }

    synchronized void start(long now) {
        startedAtMs = now;
    }

    synchronized void register(Job job, int partition) {
        if (!runId.equals(job.runId()) || job.jobId() < 0 || job.jobId() >= expected.length || expected[job.jobId()] != null) {
            throw new IllegalArgumentException("Invalid or duplicate input identity");
        }
        expected[job.jobId()] = new Expected(job, partition);
    }

    synchronized void published(int jobId, int partition, long offset) {
        Expected input = expected[jobId];
        if (input == null || input.partition != partition || published.get(jobId)) {
            throw new IllegalArgumentException("Invalid or duplicate publication confirmation");
        }
        input.offset = offset;
        published.set(jobId);
        for (Mode mode : modes.values()) {
            Observation observation = mode.first.get(jobId);
            if (observation != null && observation.inputOffset != offset) {
                mode.first.remove(jobId);
                mode.invalid++;
            }
        }
    }

    synchronized void generationFinished(long now) {
        generationFinishedAtMs = now;
    }

    synchronized void observe(String modeName, int outputPartition, long outputOffset, String value, long now) {
        Mode mode = modes.get(modeName);
        if (!mode.seen.add(outputPartition + ":" + outputOffset)) {
            return;
        }
        mode.occurrences++;
        try {
            JsonNode output = JSON.readTree(value);
            if (!validIdentityTypes(output) || !validExecutionTypes(output)) {
                mode.invalid++;
                return;
            }
            int id = output.path("jobId").asInt(-1);
            if (id < 0 || id >= expected.length || expected[id] == null) {
                mode.invalid++;
                return;
            }
            Expected input = expected[id];
            long offset = output.path("inputOffset").asLong(-1);
            long start = output.path("processingStartedMs").asLong(-1);
            long end = output.path("processingFinishedMs").asLong(-1);
            String task = output.path("taskId").asText("");
            if (!matchesContent(output, input.job, modeName) || !matchesSource(output, input, offset)
                    || !validExecution(task, start, end)) {
                mode.invalid++;
                return;
            }
            Observation observation = new Observation(task, offset, now, start, end);
            if (mode.first.putIfAbsent(id, observation) != null) {
                mode.duplicates++;
            }
        } catch (IOException | RuntimeException e) {
            mode.invalid++;
        }
    }

    private static boolean validIdentityTypes(JsonNode output) {
        return output.path("jobId").isIntegralNumber() && output.path("jobId").canConvertToInt()
                && output.path("createdAtMs").isIntegralNumber() && output.path("inputPartition").isIntegralNumber();
    }

    private static boolean validExecutionTypes(JsonNode output) {
        return output.path("inputOffset").isIntegralNumber() && output.path("processingStartedMs").isIntegralNumber()
                && output.path("processingFinishedMs").isIntegralNumber() && output.path("taskId").isTextual();
    }

    private boolean matchesContent(JsonNode output, Job job, String mode) {
        return runId.equals(output.path("runId").asText()) && mode.equals(output.path("mode").asText())
                && job.payload().equals(output.path("payload").asText())
                && job.payload().toUpperCase(Locale.ROOT).equals(output.path("result").asText())
                && job.createdAtMs() == output.path("createdAtMs").asLong(-1);
    }

    private boolean matchesSource(JsonNode output, Expected input, long offset) {
        return inputTopic().equals(output.path("inputTopic").asText())
                && input.partition == output.path("inputPartition").asInt(-1)
                && offset >= 0 && (input.offset < 0 || offset == input.offset);
    }

    private static boolean validExecution(String task, long start, long end) {
        return task.matches("[0-8]") && start >= 0 && end >= start;
    }

    synchronized boolean complete() {
        return published.cardinality() == expected.length && modes.values().stream().allMatch(mode -> unique(mode) == expected.length);
    }

    synchronized boolean valid() {
        return complete() && modes.values().stream().allMatch(mode -> mode.invalid == 0);
    }

    synchronized Map<String, Object> summaries(long now) {
        Map<String, Object> result = new LinkedHashMap<>();
        MODES.forEach(mode -> result.put(mode, summary(mode, now)));
        return result;
    }

    synchronized Map<String, Object> auditSummaries() {
        Map<String, Object> result = new LinkedHashMap<>();
        modes.forEach((name, mode) -> result.put(name, Map.of(
                "published", published.cardinality(), "unique", unique(mode), "occurrences", mode.occurrences,
                "duplicates", mode.duplicates, "invalid", mode.invalid, "unfinished", published.cardinality() - unique(mode))));
        return result;
    }

    synchronized Set<Integer> publishedPartitions() {
        Set<Integer> partitions = new HashSet<>();
        for (int id = published.nextSetBit(0); id >= 0; id = published.nextSetBit(id + 1)) {
            partitions.add(expected[id].partition);
        }
        return partitions;
    }

    synchronized long measurementFinishedAtMs() {
        if (!complete()) {
            return 0;
        }
        return modes.values().stream().flatMap(mode -> mode.first.values().stream())
                .mapToLong(output -> output.observedAtMs).max().orElse(0);
    }

    synchronized Map<String, Object> summary(String modeName, long now) {
        Mode mode = modes.get(modeName);
        List<Long> latencies = new ArrayList<>();
        Set<String> tasks = new HashSet<>();
        long lastObservation = startedAtMs;
        for (Map.Entry<Integer, Observation> entry : mode.first.entrySet()) {
            if (published.get(entry.getKey())) {
                Observation observation = entry.getValue();
                latencies.add(Math.max(0, observation.observedAtMs - expected[entry.getKey()].job.createdAtMs()));
                lastObservation = Math.max(lastObservation, observation.observedAtMs);
                tasks.add(observation.task);
            }
        }
        latencies.sort(Long::compareTo);
        long elapsed = Math.max(1, (latencies.size() == expected.length ? lastObservation : now) - startedAtMs);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("published", published.cardinality());
        result.put("unique", latencies.size());
        result.put("occurrences", mode.occurrences);
        result.put("duplicates", mode.duplicates);
        result.put("invalid", mode.invalid);
        result.put("unfinished", published.cardinality() - latencies.size());
        result.put("p95Ms", percentile(latencies, 0.95));
        result.put("p99Ms", percentile(latencies, 0.99));
        result.put("throughputPerSecond", startedAtMs == 0 ? 0 : 1000.0 * latencies.size() / elapsed);
        result.put("drainMs", generationFinishedAtMs == 0 ? 0 : Math.max(0, lastObservation - generationFinishedAtMs));
        result.put("oldestUnfinishedSeconds", oldest(mode, now) / 1000.0);
        result.put("participatingTasks", tasks.size());
        result.put("maxConcurrentTasks", concurrency(mode));
        return result;
    }

    synchronized String metrics(long now) {
        StringBuilder metrics = new StringBuilder();
        for (int partition = 0; partition < 3; partition++) {
            int count = 0;
            for (int id = published.nextSetBit(0); id >= 0; id = published.nextSetBit(id + 1)) {
                if (expected[id].partition == partition) {
                    count++;
                }
            }
            sample(metrics, "demo_published_total", "partition=\"" + partition + "\"", count);
        }
        modes.forEach((name, mode) -> {
            String labels = "mode=\"" + name + "\"";
            sample(metrics, "demo_output_occurrences_total", labels, mode.occurrences);
            sample(metrics, "demo_duplicates_total", labels, mode.duplicates);
            sample(metrics, "demo_invalid_total", labels, mode.invalid);
            sample(metrics, "demo_oldest_unfinished_seconds", labels, oldest(mode, now) / 1000.0);
            Map<String, Integer> counts = new HashMap<>();
            List<Double> seconds = new ArrayList<>();
            int[] unfinished = new int[3];
            for (int id = published.nextSetBit(0); id >= 0; id = published.nextSetBit(id + 1)) {
                Expected input = expected[id];
                Observation output = mode.first.get(id);
                if (output == null) {
                    unfinished[input.partition]++;
                } else {
                    counts.merge("partition=\"" + input.partition + "\",task=\"" + escape(output.task) + "\"", 1, Integer::sum);
                    seconds.add(Math.max(0, output.observedAtMs - input.job.createdAtMs()) / 1000.0);
                }
            }
            for (int partition = 0; partition < 3; partition++) {
                sample(metrics, "demo_unfinished", labels + ",partition=\"" + partition + "\"", unfinished[partition]);
            }
            counts.forEach((key, value) -> sample(metrics, "demo_verified_total", labels + "," + key, value));
            for (double bucket : BUCKETS) {
                sample(metrics, "demo_e2e_seconds_bucket", labels + ",le=\"" + bucket + "\"", seconds.stream().filter(value -> value <= bucket).count());
            }
            sample(metrics, "demo_e2e_seconds_bucket", labels + ",le=\"+Inf\"", seconds.size());
            sample(metrics, "demo_e2e_seconds_count", labels, seconds.size());
            sample(metrics, "demo_e2e_seconds_sum", labels, seconds.stream().mapToDouble(Double::doubleValue).sum());
        });
        return metrics.toString();
    }

    synchronized RunLedger auditCopy() {
        RunLedger copy = new RunLedger(runId, expected.length);
        copy.start(startedAtMs);
        copy.generationFinished(generationFinishedAtMs);
        for (int id = 0; id < expected.length; id++) {
            if (expected[id] != null) {
                copy.register(expected[id].job, expected[id].partition);
                if (published.get(id)) {
                    copy.published(id, expected[id].partition, expected[id].offset);
                }
            }
        }
        return copy;
    }

    String inputTopic() {
        return "demo-input-" + runId;
    }

    private int unique(Mode mode) {
        return (int) mode.first.keySet().stream().filter(published::get).count();
    }

    private long oldest(Mode mode, long now) {
        long oldest = 0;
        for (int id = published.nextSetBit(0); id >= 0; id = published.nextSetBit(id + 1)) {
            if (!mode.first.containsKey(id)) {
                oldest = Math.max(oldest, now - expected[id].job.createdAtMs());
            }
        }
        return Math.max(0, oldest);
    }

    private int concurrency(Mode mode) {
        List<Event> events = new ArrayList<>();
        mode.first.forEach((id, output) -> {
            if (published.get(id) && output.finishedAtMs > output.startedAtMs) {
                events.add(new Event(output.startedAtMs, output.task, 1));
                events.add(new Event(output.finishedAtMs, output.task, -1));
            }
        });
        events.sort(Comparator.comparingLong(Event::timeMs).thenComparingInt(Event::delta));
        Map<String, Integer> active = new HashMap<>();
        int maximum = 0;
        for (Event event : events) {
            active.compute(event.task, (task, count) -> {
                int value = (count == null ? 0 : count) + event.delta;
                return value == 0 ? null : value;
            });
            maximum = Math.max(maximum, active.size());
        }
        return maximum;
    }

    private void sample(StringBuilder out, String name, String labels, Number value) {
        out.append(name).append("{run_id=\"").append(runId).append("\",").append(labels).append("} ").append(value).append('\n');
    }

    private static Long percentile(List<Long> sorted, double quantile) {
        return sorted.isEmpty() ? null : sorted.get((int) Math.ceil(sorted.size() * quantile) - 1);
    }

    static String escape(String text) {
        return text.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"");
    }

    record Job(String runId, int jobId, String customerId, long createdAtMs, int workMs, String payload) { }

    private static final class Expected {
        private final Job job;
        private final int partition;
        private long offset = -1;

        private Expected(Job job, int partition) {
            this.job = job;
            this.partition = partition;
        }
    }

    private static final class Mode {
        private final Map<Integer, Observation> first = new HashMap<>();
        private final Set<String> seen = new HashSet<>();
        private long occurrences;
        private long duplicates;
        private long invalid;
    }

    private record Observation(String task, long inputOffset, long observedAtMs, long startedAtMs, long finishedAtMs) { }

    private record Event(long timeMs, String task, int delta) { }
}
