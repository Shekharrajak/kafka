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

import copy
import unittest
import urllib.parse

import validate_dashboard as validator


def vector(value="1"):
    return {"status": "success", "data": {"resultType": "vector", "result": [
        {"metric": {}, "value": [1000, value]}]}}


def empty():
    return {"status": "success", "data": {"resultType": "vector", "result": []}}


class DashboardValidationTest(unittest.TestCase):
    def setUp(self):
        self.report = {"runId": "test-run", "status": "FINISHED", "modes": {},
                       "audit": {"passed": True}, "sourceProgressConfirmed": True,
                       "startedAtMs": 100000, "generationFinishedAtMs": 160000,
                       "measurementFinishedAtMs": 200000, "finishedAtMs": 300000,
                       "inputTopic": "demo-input-test-run",
                       "groups": {"classic": "connect-demo-classic-test-run", "share": "demo-share-test-run"}}
        self.dashboard = {"uid": "connect-share-comparison", "panels": [
            {"id": 1, "title": "Verified output", "targets": [
                {"refId": "A", "expr": 'sum(rate(demo_verified_total{run_id="$run_id"}[30s]))'}]}]}

    def test_rendering_substitutes_only_known_variables_and_validated_run_ids(self):
        rendered = validator.render_expression('metric{run_id="${run_id}"}[$__range:5s]', "test-run", 40500)
        self.assertEqual('metric{run_id="test-run"}[41s:5s]', rendered)
        with self.assertRaises(ValueError):
            validator.render_expression("$unexpected", "test-run", 1000)
        with self.assertRaises(ValueError):
            validator.render_expression("$run_id", 'bad"selector', 1000)

    def test_full_window_uses_measurement_completion_and_active_window_precedes_drain(self):
        windows = validator.windows_for(self.report)
        self.assertEqual("activeWindow", windows[0]["name"])
        self.assertEqual(145000, windows[0]["endMs"])
        self.assertEqual(205000, windows[1]["endMs"])
        report = {**self.report, "generationFinishedAtMs": 150000, "measurementFinishedAtMs": 149999}
        self.assertEqual(155000, validator.windows_for(report)[1]["endMs"])
        self.assertEqual(self.report, validator.report_from({"report": self.report}))

    def test_empty_required_metric_fails_and_zero_remains_a_valid_observation(self):
        expression = 'demo_verified_total{run_id="test-run",mode="share"}'
        self.assertEqual("FAILED", validator.assess(empty(), expression, self.report)["status"])
        result = validator.assess(vector("0"), expression, self.report)
        self.assertEqual("PASSED", result["status"])
        self.assertEqual(0, result["maximum"])

    def test_optional_native_lag_needs_explicit_unavailability_and_does_not_hide_ack_metrics(self):
        expression = 'demo_source_lag{run_id="test-run",mode="share"}'
        self.assertEqual("FAILED", validator.assess(empty(), expression, self.report)["status"])
        report = {**self.report, "nativeMetrics": {"share": {"lagAvailable": False}}}
        self.assertEqual("NOT_APPLICABLE", validator.assess(empty(), expression, report)["status"])
        acknowledgement = "rate(kafka_share_acknowledgements_total[30s])"
        self.assertEqual("FAILED", validator.assess(empty(), acknowledgement, report)["status"])
        self.assertEqual("broker-wide share acknowledgements", validator.metric_scope(acknowledgement))
        self.assertEqual("share group", validator.metric_scope('kafka_share_inflight_records{group="demo-share-test-run"}'))

    def test_instant_histogram_without_completions_is_unknown_only_when_count_confirms_it(self):
        item = ({"id": 2, "title": "p95"}, {"refId": "A", "instant": True,
                "expr": 'histogram_quantile(0.95,sum by(le)(rate(demo_e2e_seconds_bucket{run_id="$run_id",mode="share"}[30s])))'})

        def request(base, path):
            expression = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)["query"][0]
            return vector("0") if "demo_e2e_seconds_count" in expression else vector("NaN")

        result = validator.query_target(item, validator.windows_for(self.report)[1], self.report, "http://prom", request)
        self.assertEqual("NOT_APPLICABLE", result["status"])
        self.assertIn("completionCountQuery", result)
        bad = validator.query_target(item, validator.windows_for(self.report)[1], self.report,
                                     "http://prom", lambda *args: empty())
        self.assertEqual("FAILED", bad["status"])

    def test_nonfinite_or_impossible_ratios_fail_without_fabricated_values(self):
        self.assertEqual("FAILED", validator.assess(vector("NaN"), "demo_members", self.report)["status"])
        self.assertEqual("FAILED", validator.assess(vector("-1"), "demo_unfinished", self.report)["status"])
        self.assertEqual("FAILED", validator.assess(vector("1.2"), "demo_jvm_process_cpu_ratio", self.report)["status"])

    def test_completed_work_cannot_pass_with_a_stuck_zero_activity_metric(self):
        report = {**self.report, "modes": {"share": {"unique": 100}}}
        expression = 'sum(rate(demo_verified_total{mode="share"}[30s]))'
        self.assertEqual("FAILED", validator.assess(vector("0"), expression, report)["status"])
        self.assertEqual("PASSED", validator.assess(vector("2"), expression, report)["status"])

    def test_prometheus_success_cannot_hide_a_failed_grafana_datasource(self):
        def request(base, path):
            if base == "http://grafana":
                raise OSError("Grafana unavailable")
            return vector()

        result = validator.validate(self.dashboard, self.report, "http://prom", "http://grafana", request)
        self.assertFalse(result["passed"])
        self.assertEqual(0, result["querySummary"]["failed"])
        self.assertTrue(any("datasource query failed" in issue for issue in result["issues"]))

    def test_provisioned_panel_drift_is_reported_and_raw_queries_are_retained(self):
        modified = copy.deepcopy(self.dashboard)
        modified["panels"][0]["title"] = "Unexpected dashboard"

        def request(base, path):
            if path.startswith("/api/dashboards/"):
                return {"dashboard": modified, "meta": {"provisioned": True}}
            if path.endswith("/health"):
                return {"status": "OK"}
            return vector()

        result = validator.validate(self.dashboard, self.report, "http://prom", "http://grafana", request)
        self.assertFalse(result["passed"])
        self.assertIn("response", result["queries"][0])
        self.assertNotIn("$run_id", result["queries"][0]["expression"])
        self.assertTrue(any("differ" in issue for issue in result["issues"]))

    def test_disabled_grafana_query_cannot_pass_direct_prometheus_validation(self):
        modified = copy.deepcopy(self.dashboard)

        def request(base, path):
            if path.startswith("/api/dashboards/"):
                return {"dashboard": modified, "meta": {"provisioned": True}}
            if path.endswith("/health"):
                return {"status": "OK"}
            return vector()

        modified["panels"][0]["targets"][0]["hide"] = False
        enabled = validator.validate(self.dashboard, self.report, "http://prom", "http://grafana", request)
        self.assertTrue(enabled["passed"])
        modified["panels"][0]["targets"][0]["hide"] = True
        disabled = validator.validate(self.dashboard, self.report, "http://prom", "http://grafana", request)
        self.assertFalse(disabled["passed"])
        self.assertEqual(0, disabled["querySummary"]["failed"])
        self.assertTrue(any("differ" in issue for issue in disabled["issues"]))


if __name__ == "__main__":
    unittest.main()
