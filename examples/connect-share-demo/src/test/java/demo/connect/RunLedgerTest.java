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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunLedgerTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void outputBeforePublicationConfirmationIsCountedOnlyAfterConfirmation() {
        RunLedger ledger = ledger(1);
        ledger.observe("share", 0, 0, output(0, "0", 1000, 1020).toString(), 1100);
        assertEquals(0, ledger.summary("share", 1100).get("unique"));
        assertNull(ledger.summary("share", 1100).get("p95Ms"));
        ledger.published(0, 0, 0);
        assertEquals(1, ledger.summary("share", 1100).get("unique"));
        assertEquals(100L, ledger.summary("share", 1100).get("p95Ms"));
    }

    @Test
    void physicalDuplicatesAreRetainedButObserverReplayIsNotDoubleCounted() {
        RunLedger ledger = ledger(1);
        ledger.published(0, 0, 0);
        String value = output(0, "0", 1000, 1020).toString();
        ledger.observe("share", 0, 0, value, 1100);
        ledger.observe("share", 0, 0, value, 1110);
        ledger.observe("share", 0, 1, value, 1120);
        Map<String, Object> summary = ledger.summary("share", 1200);
        assertEquals(1, summary.get("unique"));
        assertEquals(2L, summary.get("occurrences"));
        assertEquals(1L, summary.get("duplicates"));
        assertEquals(0, summary.get("unfinished"));
    }

    @Test
    void corruptAndWrongInputIdentityCannotHideMissingWork() {
        RunLedger ledger = ledger(1);
        ledger.published(0, 0, 0);
        ledger.observe("share", 0, 0, output(0, "0", 1000, 1020).put("result", "corrupt").toString(), 1100);
        ledger.observe("share", 0, 1, output(0, "0", 1000, 1020).put("inputOffset", 9).toString(), 1100);
        ledger.observe("share", 0, 2, output(7, "0", 1000, 1020).toString(), 1100);
        assertEquals(3L, ledger.summary("share", 1200).get("invalid"));
        assertEquals(1, ledger.summary("share", 1200).get("unfinished"));
        assertFalse(ledger.complete());
    }

    @Test
    void participationDoesNotImplyConcurrentProcessing() {
        RunLedger sequential = ledger(4);
        RunLedger concurrent = ledger(4);
        for (int id = 0; id < 4; id++) {
            sequential.published(id, id % 3, id);
            concurrent.published(id, id % 3, id);
            sequential.observe("share", 0, id, output(id, Integer.toString(id), 1000 + 20L * id, 1020 + 20L * id).toString(), 1200);
            concurrent.observe("share", 0, id, output(id, Integer.toString(id), 1000, 1020).toString(), 1200);
        }
        assertEquals(4, sequential.summary("share", 1200).get("participatingTasks"));
        assertEquals(1, sequential.summary("share", 1200).get("maxConcurrentTasks"));
        assertEquals(4, concurrent.summary("share", 1200).get("maxConcurrentTasks"));
    }

    @Test
    void independentAuditStartsWithoutObservedOutputsAndRequiresBothModes() {
        RunLedger original = ledger(1);
        original.published(0, 0, 0);
        original.observe("share", 0, 0, output(0, "0", 1000, 1020).toString(), 1100);
        RunLedger audit = original.auditCopy();
        assertEquals(0, audit.summary("share", 1200).get("unique"));
        audit.observe("share", 0, 0, output(0, "0", 1000, 1020).toString(), 1200);
        assertFalse(audit.valid());
        audit.observe("classic", 0, 0, output(0, "0", 1000, 1020).put("mode", "classic").toString(), 1200);
        assertTrue(audit.valid());
        assertEquals(1200L, audit.measurementFinishedAtMs());
        assertFalse(audit.auditSummaries().get("share").toString().contains("p95"));
        assertTrue(audit.metrics(1200).contains("demo_unfinished{run_id=\"test\",mode=\"classic\",partition=\"0\"} 0"));
    }

    @Test
    void oldestUnfinishedAgeRemainsVisibleWhenCompletedLatencyLooksHealthy() {
        RunLedger ledger = ledger(2);
        ledger.published(0, 0, 0);
        ledger.published(1, 1, 1);
        ledger.observe("share", 0, 0, output(0, "0", 1000, 1020).toString(), 1100);
        assertEquals(100L, ledger.summary("share", 11_000).get("p95Ms"));
        assertEquals(10.0, ledger.summary("share", 11_000).get("oldestUnfinishedSeconds"));
    }

    private static RunLedger ledger(int count) {
        RunLedger ledger = new RunLedger("test", count);
        ledger.start(1000);
        for (int id = 0; id < count; id++) {
            ledger.register(new RunLedger.Job("test", id, "customer", 1000, 20, "document-" + id), id % 3);
        }
        return ledger;
    }

    private static ObjectNode output(int id, String task, long start, long finish) {
        ObjectNode result = JSON.createObjectNode();
        result.put("runId", "test");
        result.put("jobId", id);
        result.put("createdAtMs", 1000);
        result.put("payload", "document-" + id);
        result.put("result", "DOCUMENT-" + id);
        result.put("mode", "share");
        result.put("taskId", task);
        result.put("inputTopic", "demo-input-test");
        result.put("inputPartition", id % 3);
        result.put("inputOffset", id);
        result.put("processingStartedMs", start);
        result.put("processingFinishedMs", finish);
        return result;
    }
}
