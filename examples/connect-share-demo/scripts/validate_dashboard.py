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

"""Validate the provisioned Grafana dashboard against real Prometheus samples."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import math
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "dashboard" / "dashboard.json"
IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9-]{0,39}")


def get_json(base, path, timeout=15):
    try:
        with urllib.request.urlopen(base.rstrip("/") + path, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        error.close()
        raise


def report_from(value):
    if isinstance(value, dict) and isinstance(value.get("report"), dict):
        value = value["report"]
    if (not isinstance(value, dict) or not IDENTIFIER.fullmatch(str(value.get("runId", "")))
            or not isinstance(value.get("modes"), dict)):
        raise ValueError("Expected a controller report or runner evidence containing report")
    return value


def windows_for(report):
    started = report.get("startedAtMs", 0)
    generated = report.get("generationFinishedAtMs", 0)
    completed = report.get("measurementFinishedAtMs") or report.get("finishedAtMs", 0)
    if not all(isinstance(value, (int, float)) and math.isfinite(value)
               for value in (started, generated, completed)):
        raise ValueError("Run timestamps must be finite numbers")
    if not 0 < started <= generated or completed < started:
        raise ValueError("A completed run with ordered generation/measurement timestamps is required")
    windows = [{"name": "fullRun", "startMs": started - 5000, "endMs": max(completed, generated) + 5000}]
    if generated - started >= 35000:
        windows.insert(0, {"name": "activeWindow", "startMs": started - 5000,
                           "endMs": min(started + 45000, generated - 1)})
    return windows


def render_expression(expression, run_id, duration_ms):
    if not IDENTIFIER.fullmatch(run_id):
        raise ValueError("Invalid run ID")
    duration = str(max(1, math.ceil(duration_ms / 1000))) + "s"
    values = {"run_id": run_id, "__range": duration}

    def replace(match):
        name = match.group(1) or match.group(2)
        if name not in values:
            raise ValueError("Unsupported dashboard variable: " + name)
        return values[name]

    return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)", replace, expression)


def targets(dashboard):
    for panel in dashboard.get("panels", []):
        for query in panel.get("targets", []):
            if "expr" in query and not query.get("hide", False):
                yield panel, query


def signature(dashboard):
    return {panel["id"]: {"title": panel["title"], "datasource": panel.get("datasource"), "targets": [
        {**{key: query.get(key) for key in ("refId", "expr", "instant", "range", "format", "datasource")},
         "hide": bool(query.get("hide", False))}
        for query in panel.get("targets", []) if "expr" in query
    ]} for panel in dashboard.get("panels", [])}


def metric_scope(expression):
    if "kafka_share_acknowledgements_total" in expression:
        return "broker-wide share acknowledgements"
    if "kafka_share_" in expression:
        return "share group"
    modes = re.findall(r'mode="(classic|share)"', expression)
    return modes[0] if len(set(modes)) == 1 else "common or both modes"


def series_values(response):
    if response.get("status") != "success":
        raise ValueError("Prometheus query failed: " + str(response.get("error", response)))
    data = response.get("data", {})
    kind = data.get("resultType")
    result = data.get("result")
    if kind == "scalar":
        return [result]
    if kind not in {"vector", "matrix"} or not isinstance(result, list):
        raise ValueError("Expected a Prometheus vector, matrix or scalar")
    values = []
    for series in result:
        if kind == "vector":
            values.append(series["value"])
        else:
            values.extend(series["values"])
    return values


def unavailable_reason(expression, report):
    if "demo_source_lag" not in expression:
        return None
    mode = "share" if 'mode="share"' in expression else "classic"
    metadata = report.get("nativeMetrics", {}).get(mode, {})
    if metadata.get("lagAvailable") is False:
        return mode + " native lag explicitly reported unavailable by the controller"
    return None


def requires_positive(expression, report):
    if "max_over_time" in expression:
        return False
    if "demo_published_total" in expression:
        return report.get("records", 0) > 0
    if "demo_verified_total" in expression:
        names = re.findall(r'mode="(classic|share)"', expression) or ["classic", "share"]
        return any(report.get("modes", {}).get(mode, {}).get("unique", 0) > 0 for mode in names)
    return False


def assess(response, expression, report, require_activity=True):
    values = series_values(response)
    numbers = [float(value[1]) for value in values]
    finite = [value for value in numbers if math.isfinite(value)]
    details = {"samples": len(numbers), "finiteSamples": len(finite),
               "nonFiniteSamples": len(numbers) - len(finite),
               "minimum": min(finite) if finite else None,
               "maximum": max(finite) if finite else None}
    if not finite:
        reason = unavailable_reason(expression, report)
        details.update(status="NOT_APPLICABLE" if reason else "FAILED",
                       reason=reason or "No finite samples; absent data was not replaced with zero")
        return details
    if any(value < -0.000001 for value in finite):
        details.update(status="FAILED", reason="Negative value for a nonnegative demo measurement")
    elif "histogram_quantile" not in expression and len(finite) != len(numbers):
        details.update(status="FAILED", reason="Unexpected nonfinite samples")
    elif ("demo_jvm_process_cpu_ratio" in expression or "demo_jvm_heap_max_bytes" in expression) and max(finite) > 1.000001:
        details.update(status="FAILED", reason="Ratio exceeds one")
    elif require_activity and requires_positive(expression, report) and max(finite) <= 0:
        details.update(status="FAILED", reason="Run report proves completed work, but this activity query never becomes positive")
    else:
        details["status"] = "PASSED"
    return details


def query_target(item, window, report, prometheus, request=get_json):
    panel, query = item
    expression = render_expression(query["expr"], report["runId"], window["endMs"] - window["startMs"])
    result = {"window": window["name"], "panelId": panel["id"], "panel": panel["title"],
              "refId": query["refId"], "scope": metric_scope(expression), "expression": expression}
    try:
        if query.get("instant", False):
            endpoint = "/api/v1/query"
            parameters = {"query": expression, "time": window["endMs"] / 1000}
        else:
            endpoint = "/api/v1/query_range"
            parameters = {"query": expression, "start": window["startMs"] / 1000,
                          "end": window["endMs"] / 1000, "step": 5}
        result["response"] = request(prometheus, endpoint + "?" + urllib.parse.urlencode(parameters))
        result.update(assess(result["response"], expression, report, require_activity=not query.get("instant", False)))
        if (result["status"] == "FAILED" and result.get("finiteSamples") == 0
                and query.get("instant", False) and "histogram_quantile" in expression
                and "max_over_time" not in expression):
            selector = re.search(r"demo_e2e_seconds_bucket(\{[^}]+\})", expression)
            if selector:
                count_query = "sum(increase(demo_e2e_seconds_count" + selector.group(1) + "[30s]))"
                count_response = request(prometheus, "/api/v1/query?" + urllib.parse.urlencode({
                    "query": count_query, "time": window["endMs"] / 1000}))
                result["completionCountQuery"] = {"expression": count_query, "response": count_response}
                counts = [float(item[1]) for item in series_values(count_response)]
                if counts and all(math.isfinite(count) and count == 0 for count in counts):
                    result.update(status="NOT_APPLICABLE", reason="No completions in this rolling histogram window; count series confirms zero")
    except (OSError, ValueError, KeyError, TypeError) as error:
        result.update(status="FAILED", reason=str(error))
    return result


def grafana_checks(grafana, expected, expression, at_ms, request=get_json):
    checks = {}
    issues = []
    try:
        response = request(grafana, "/api/dashboards/uid/" + expected["uid"])
        actual = response["dashboard"]
        checks["dashboard"] = {"uid": actual.get("uid"), "provisioned": response.get("meta", {}).get("provisioned"),
                               "panels": len(actual.get("panels", []))}
        if actual.get("uid") != expected["uid"] or signature(actual) != signature(expected):
            issues.append("Grafana panel titles or PromQL targets differ from the checked-in dashboard")
        if response.get("meta", {}).get("provisioned") is not True:
            issues.append("Grafana did not identify the dashboard as provisioned")
    except (OSError, ValueError, KeyError, TypeError) as error:
        checks["dashboard"] = {"error": str(error)}
        issues.append("Grafana dashboard API could not be verified")
    try:
        checks["datasourceHealth"] = request(grafana, "/api/datasources/uid/demo-prom/health")
        if str(checks["datasourceHealth"].get("status", "")).upper() not in {"OK", "SUCCESS"}:
            issues.append("Grafana datasource health returned an unsuccessful status")
    except urllib.error.HTTPError as error:
        code = error.code
        error.close()
        checks["datasourceHealth"] = {"unavailable": code in {401, 403, 404, 405}, "error": str(error)}
        if code not in {401, 403, 404, 405}:
            issues.append("Grafana datasource health endpoint failed")
    except (OSError, ValueError) as error:
        checks["datasourceHealth"] = {"error": str(error)}
        issues.append("Grafana datasource health could not be checked")
    try:
        path = "/api/datasources/proxy/uid/demo-prom/api/v1/query?" + urllib.parse.urlencode({"query": expression, "time": at_ms / 1000})
        checks["datasourceQuery"] = request(grafana, path)
        samples = series_values(checks["datasourceQuery"])
        if not any(math.isfinite(float(sample[1])) for sample in samples):
            issues.append("Grafana datasource proxy returned no finite selected-run samples")
    except (OSError, ValueError, KeyError, TypeError) as error:
        checks["datasourceQuery"] = {"error": str(error)}
        issues.append("Grafana datasource query failed; direct Prometheus success cannot replace it")
    return checks, issues


def validate(dashboard, report, prometheus, grafana, request=get_json):
    windows = windows_for(report)
    issues = []
    if report.get("status") != "FINISHED" or report.get("audit", {}).get("passed") is not True:
        issues.append("The selected run has not passed its independent output audit")
    if not report.get("sourceProgressConfirmed", False):
        issues.append("The selected run has not confirmed native source progress")
    if not any(window["name"] == "activeWindow" for window in windows):
        issues.append("Generation lasted less than 35s; no complete active 30s rate window")
    native = report.get("groups", {}).get("share")
    if native != "demo-share-" + report["runId"]:
        issues.append("Dashboard share-group selector does not match the controller's actual group identity")
    work = [(item, window) for window in windows for item in targets(dashboard)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        outputs = list(pool.map(lambda pair: query_target(pair[0], pair[1], report, prometheus, request), work))
    failed = sum(output["status"] == "FAILED" for output in outputs)
    if failed:
        issues.append(str(failed) + " dashboard queries failed")
    selected = windows[0]
    expression = 'sum(rate(demo_verified_total{run_id="' + report["runId"] + '"}[30s]))'
    grafana_result, grafana_issues = grafana_checks(grafana, dashboard, expression, selected["endMs"], request)
    issues.extend(grafana_issues)
    return {"runId": report["runId"], "validatedAtMs": round(time.time() * 1000),
            "prometheus": prometheus, "grafana": grafana, "windows": windows,
            "inputTopic": report.get("inputTopic"), "groups": report.get("groups"),
            "queries": outputs, "grafanaChecks": grafana_result, "issues": issues,
            "querySummary": {"total": len(outputs), "passed": sum(item["status"] == "PASSED" for item in outputs),
                             "notApplicable": sum(item["status"] == "NOT_APPLICABLE" for item in outputs), "failed": failed},
            "passed": not issues}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--report", type=Path)
    source.add_argument("--run-id")
    parser.add_argument("--url", default="http://localhost:8090")
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--grafana", default="http://localhost:3000")
    parser.add_argument("--results", type=Path, default=ROOT / "results")
    args = parser.parse_args(argv)
    if args.run_id and not IDENTIFIER.fullmatch(args.run_id):
        parser.error("Invalid run ID")
    value = json.loads(args.report.read_text()) if args.report else get_json(args.url, "/runs/" + args.run_id)
    report = report_from(value)
    dashboard = json.loads(DASHBOARD.read_text())
    result = validate(dashboard, report, args.prometheus, args.grafana)
    args.results.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = args.results / (report["runId"] + "-dashboard-" + stamp + "-" + uuid.uuid4().hex[:6] + ".json")
    with path.open("x", encoding="utf-8") as output:
        json.dump(result, output, indent=2, allow_nan=False)
        output.write("\n")
    print(("PASSED" if result["passed"] else "FAILED") + ": " + str(path))
    print(json.dumps(result["querySummary"]))
    for issue in result["issues"]:
        print(issue, file=sys.stderr)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)
