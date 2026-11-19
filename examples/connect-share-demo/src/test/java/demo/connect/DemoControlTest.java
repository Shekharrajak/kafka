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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoControlTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path directory;

    @Test
    void retainedStartWithoutFinalReportBlocksAdmissionAfterRestart() throws IOException {
        Files.writeString(directory.resolve("unknown.request.json"), "{\"runId\":\"unknown\"}");
        Map<String, Object> report = DemoControl.storedReports(directory).get("unknown");
        assertEquals("FAILED", report.get("status"));
        assertEquals(true, report.get("requiresInspection"));
        assertTrue(report.get("error").toString().contains("unresolved"));
    }

    @Test
    void explicitSkewPlacesNinetyPercentOfJobsOnPartitionZero() throws IOException {
        DemoControl.Request request = DemoControl.Request.parse(JSON.readTree("{\"runId\":\"skew\",\"scenario\":\"skewed\"}"));
        int[] count = new int[3];
        for (int id = 0; id < 1000; id++) {
            count[request.partition(id)]++;
        }
        assertEquals(900, count[0]);
        assertEquals(50, count[1]);
        assertEquals(50, count[2]);
    }

    @Test
    void rejectsUnsafeIdentityAndUnboundedWorkload() {
        assertThrows(IllegalArgumentException.class, () -> DemoControl.Request.parse(JSON.readTree("{\"runId\":\"../existing\"}")));
        assertThrows(IllegalArgumentException.class, () -> DemoControl.Request.parse(JSON.readTree("{\"runId\":\"test\",\"records\":50001}")));
        assertThrows(IllegalArgumentException.class, () -> DemoControl.Request.parse(JSON.readTree("{\"runId\":\"test\",\"rate\":0}")));
        assertThrows(IllegalArgumentException.class, () -> DemoControl.Request.parse(JSON.readTree("{\"runId\":\"test\",\"tasks\":10}")));
    }
}
