#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

import demo
import fault_worker


class FakeClock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class DemoRunnerTest(unittest.TestCase):
    def setUp(self):
        self.payload = {"runId": "test-run", "scenario": "skewed", "rate": 10,
                        "records": 10, "workMs": 20, "tasks": 9}

    def test_lost_start_response_is_recovered_without_a_second_post(self):
        calls = []

        def request(base, method, path, payload=None):
            calls.append((method, path))
            if method == "POST":
                raise TimeoutError("response lost after server accepted the run")
            return {"runId": "test-run", "status": "FINISHED",
                    "modes": {"share": {"maxConcurrentTasks": 6}}}

        with tempfile.TemporaryDirectory() as directory:
            code, path, evidence = demo.run_one("http://control", self.payload, directory,
                                                request=request, min_share_parallelism=4)
            self.assertEqual(0, code)
            self.assertEqual("FINISHED", evidence["status"])
            self.assertEqual(evidence, json.loads(path.read_text()))
        self.assertEqual(1, sum(method == "POST" for method, _ in calls))

    def test_unknown_start_is_preserved_when_reads_cannot_find_the_run(self):
        clock = FakeClock()
        calls = []

        def request(base, method, path, payload=None):
            calls.append((method, path))
            if method == "POST":
                raise TimeoutError("unknown start")
            if path.startswith("/runs/"):
                raise urllib.error.HTTPError(base + path, 404, "not found", {}, None)
            return {"current": None, "runs": []}

        with tempfile.TemporaryDirectory() as directory:
            code, path, evidence = demo.run_one(
                "http://control", self.payload, directory, timeout=10,
                recovery_seconds=3, interval=1, request=request,
                clock=clock, sleep=clock.sleep,
            )
            self.assertEqual(2, code)
            self.assertEqual("UNKNOWN_START", evidence["status"])
            self.assertEqual("test-run", json.loads(path.read_text())["runId"])
        self.assertEqual(1, sum(method == "POST" for method, _ in calls))

    def test_existing_evidence_prevents_any_start_request(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test-run.json"
            path.write_text("retained evidence")
            with self.assertRaises(FileExistsError):
                demo.run_one("http://control", self.payload, directory,
                             request=lambda *args: self.fail("must not issue a request"))
            self.assertEqual("retained evidence", path.read_text())

    def test_finished_run_does_not_pass_requested_parallelism_without_evidence(self):
        def request(base, method, path, payload=None):
            return {"runId": "test-run", "status": "FINISHED",
                    "maxConcurrentTasks": {"share": 3}}

        with tempfile.TemporaryDirectory() as directory:
            code, _, evidence = demo.run_one("http://control", self.payload, directory,
                                             request=request, min_share_parallelism=4)
        self.assertEqual(1, code)
        self.assertEqual("VALIDATION_FAILED", evidence["status"])
        self.assertFalse(evidence["parallelismCheck"]["passed"])

    def test_known_rejection_is_saved_without_polling_or_resubmitting(self):
        calls = []

        def request(base, method, path, payload=None):
            calls.append((method, path))
            raise urllib.error.HTTPError(base + path, 409, "active run", {}, None)

        with tempfile.TemporaryDirectory() as directory:
            code, path, evidence = demo.run_one("http://control", self.payload, directory,
                                                request=request)
            self.assertEqual("REJECTED", json.loads(path.read_text())["status"])
        self.assertEqual(2, code)
        self.assertEqual("REJECTED", evidence["status"])
        self.assertEqual([("POST", "/runs")], calls)

    def test_report_links_keep_active_generation_separate_from_later_drain(self):
        urls = demo.grafana_urls("http://grafana", "test-run", {
            "startedAtMs": 100000, "generationFinishedAtMs": 160000,
            "measurementFinishedAtMs": 250000, "finishedAtMs": 400000,
        })
        self.assertIn("to=145000", urls["activeWindow"])
        self.assertIn("to=255000", urls["fullRun"])
        self.assertEqual({"live"}, set(demo.grafana_urls("http://grafana", "test-run", None)))


class DashboardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = Path(__file__).resolve().parents[1] / "dashboard"
        cls.dashboard = json.loads((cls.directory / "dashboard.json").read_text())

    def test_generated_dashboard_is_current_and_uses_distinct_panel_ids(self):
        spec = importlib.util.spec_from_file_location("build_dashboard", self.directory / "build_dashboard.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.build(), self.dashboard)
        identifiers = [panel["id"] for panel in self.dashboard["panels"]]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        self.assertEqual("connect-share-comparison", self.dashboard["uid"])

    def test_paired_panels_select_the_same_run_and_the_correct_mode(self):
        checked = 0
        for panel in self.dashboard["panels"]:
            title = panel["title"]
            if title.endswith(" - ordinary") or title.endswith(" - share"):
                mode = "classic" if title.endswith(" - ordinary") else "share"
                expression = panel["targets"][0]["expr"]
                self.assertIn('mode="' + mode + '"', expression)
                if "demo_jvm" not in expression:
                    self.assertIn('run_id="$run_id"', expression)
                checked += 1
        self.assertGreater(checked, 20)

    def test_backlog_latency_and_missing_scrape_semantics_remain_distinct(self):
        panels = self.dashboard["panels"]
        active = next(panel for panel in panels if panel["title"] == "Sampled concurrent processing tasks - share")
        self.assertIn('up{job="connect-tasks"} == 1', active["targets"][0]["expr"])
        for panel in panels:
            for item in panel.get("targets", []):
                self.assertNotIn("or vector(0)", item["expr"])
                self.assertNotIn("jobId", item["expr"])
        titles = {panel["title"] for panel in panels}
        self.assertIn("Native share lag", titles)
        self.assertIn("Committed-offset lag", titles)
        self.assertIn("Oldest unfinished job age - share", titles)


class WorkerFaultTest(unittest.TestCase):
    def test_selection_requires_running_task_fresh_processing_and_mode_allowlist(self):
        status = {"tasks": [{"state": "RUNNING", "worker_id": "share-2:8083"},
                            {"state": "FAILED", "worker_id": "share-3:8083"}]}
        metrics = [{"metric": {"worker": worker}, "value": [0, value]}
                   for worker, value in (("share-2", "1"), ("share-3", "3"), ("broker", "9"))]
        self.assertEqual("share-2", fault_worker.choose_worker("share", status, metrics))
        with self.assertRaises(ValueError):
            fault_worker.choose_worker("classic", status, metrics)

    def test_foreign_or_broker_container_is_rejected_before_mutation(self):
        labels = {"com.docker.compose.project": fault_worker.PROJECT,
                  "com.docker.compose.service": "share-2",
                  "com.docker.compose.project.config_files": str(fault_worker.COMPOSE)}
        fault_worker.verify_labels("share-2", labels)
        with self.assertRaises(ValueError):
            fault_worker.verify_labels("broker", labels)
        with self.assertRaises(ValueError):
            fault_worker.verify_labels("share-2", {**labels, "com.docker.compose.project": "other-demo"})

    def test_uncertain_kill_still_attempts_to_restart_the_verified_container(self):
        calls = []
        container = "b" * 64

        def request(base, method, path, payload=None):
            if path == "/state":
                return {"current": {"runId": "test-run", "status": "GENERATING"}}
            if path.endswith("/status"):
                return {"tasks": [{"state": "RUNNING", "worker_id": "share-2:8083"}]}
            return {"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": {"worker": "share-2"}, "value": [0, "1"]}]}}

        def command(arguments, timeout=30):
            calls.append(arguments)
            if arguments[1] == "compose":
                return container
            if arguments[1] == "inspect":
                return json.dumps({"com.docker.compose.project": fault_worker.PROJECT,
                                   "com.docker.compose.service": "share-2",
                                   "com.docker.compose.project.config_files": str(fault_worker.COMPOSE)})
            if arguments[1] == "kill":
                raise TimeoutError("kill reply lost")
            return container

        with tempfile.TemporaryDirectory() as directory:
            with patch.object(fault_worker, "request_json", request), patch.object(fault_worker, "command", command):
                code = fault_worker.main(["--mode", "share", "--results", directory])
            files = list(Path(directory).glob("*.json"))
            self.assertEqual(1, len(files))
            evidence = json.loads(files[0].read_text())
        self.assertEqual(1, code)
        self.assertEqual("FAULT_OUTCOME_UNKNOWN", evidence["status"])
        self.assertEqual(["docker", "start", container], calls[-1])


if __name__ == "__main__":
    unittest.main()
