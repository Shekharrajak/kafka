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

"""Run a finite comparison without retrying an uncertain generation start."""

import argparse
import datetime
import json
import math
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


TERMINAL = {"FINISHED", "FAILED"}
RESULTS = Path(__file__).resolve().parents[1] / "results"


def request_json(base, method, path, payload=None, timeout=15):
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        base.rstrip("/") + path, data=body, method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def report_for(state, run_id):
    if not isinstance(state, dict):
        return None
    candidates = [state, state.get("current")]
    history = state.get("runs", [])
    candidates.extend(history.values() if isinstance(history, dict) else history)
    return next((item for item in candidates
                 if isinstance(item, dict) and item.get("runId") == run_id), None)


def read_report(base, run_id, request=request_json):
    try:
        report = request(base, "GET", "/runs/" + urllib.parse.quote(run_id, safe=""))
        return report_for(report, run_id)
    except urllib.error.HTTPError as error:
        error.close()
        if error.code != 404:
            raise
    return report_for(request(base, "GET", "/state"), run_id)


def save_evidence(path, evidence):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8") as output:
            json.dump(evidence, output, indent=2, allow_nan=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def share_parallelism(report):
    modes = report.get("modes", {})
    share = modes.get("share", {}) if isinstance(modes, dict) else {}
    value = share.get("maxConcurrentTasks")
    if value is None:
        value = report.get("maxConcurrentTasks", {})
        value = value.get("share") if isinstance(value, dict) else None
    return value if isinstance(value, (int, float)) and math.isfinite(value) else None


def run_one(base, payload, output_dir=RESULTS, timeout=900, interval=2,
            recovery_seconds=20, min_share_parallelism=None, request=request_json,
            sleep=time.sleep, clock=time.monotonic):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / (payload["runId"] + ".json")
    evidence = {
        "runId": payload["runId"], "request": payload, "controller": base,
        "startedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "status": "SUBMITTING", "report": None, "errors": [],
    }
    with path.open("x", encoding="utf-8") as reserved:
        json.dump(evidence, reserved, indent=2)
        reserved.flush()
        os.fsync(reserved.fileno())

    deadline = clock() + timeout
    try:
        try:
            evidence["startResponse"] = request(base, "POST", "/runs", payload)
            evidence["status"] = "ACCEPTED"
        except urllib.error.HTTPError as error:
            evidence["errors"].append(str(error))
            error.close()
            if 400 <= error.code < 500:
                evidence["status"] = "REJECTED"
                save_evidence(path, evidence)
                return 2, path, evidence
            evidence["status"] = "UNKNOWN_START"
        except (OSError, ValueError) as error:
            evidence["status"] = "UNKNOWN_START"
            evidence["errors"].append(str(error))
        save_evidence(path, evidence)
        recovering = evidence["status"] == "UNKNOWN_START"
        recovery_deadline = min(deadline, clock() + recovery_seconds)

        while clock() < deadline:
            try:
                report = read_report(base, payload["runId"], request)
                if report is not None:
                    recovering = False
                    evidence["report"] = report
                    status = str(report.get("status", "UNKNOWN")).upper()
                    evidence["status"] = status
                    if status in TERMINAL:
                        passed = status == "FINISHED"
                        if passed and min_share_parallelism is not None:
                            observed = share_parallelism(report)
                            passed = observed is not None and observed >= min_share_parallelism
                            evidence["parallelismCheck"] = {
                                "minimum": min_share_parallelism,
                                "observed": observed, "passed": passed,
                            }
                            if not passed:
                                evidence["status"] = "VALIDATION_FAILED"
                        save_evidence(path, evidence)
                        return (0 if passed else 1), path, evidence
                elif recovering and clock() >= recovery_deadline:
                    break
            except (OSError, ValueError) as error:
                evidence["errors"] = (evidence["errors"] + [str(error)])[-10:]
            save_evidence(path, evidence)
            if recovering and clock() >= recovery_deadline:
                break
            sleep(interval)

        evidence["status"] = "UNKNOWN_START" if recovering else "TIMED_OUT"
        evidence["note"] = "Inspect the existing run; generation was not resubmitted."
        save_evidence(path, evidence)
        return 2, path, evidence
    except KeyboardInterrupt:
        evidence["status"] = "INTERRUPTED"
        evidence["note"] = "The controller may still be running; generation was not resubmitted."
        save_evidence(path, evidence)
        return 130, path, evidence


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return number


def run_id(value):
    if not re.fullmatch(r"[a-z0-9-]{1,40}", value):
        raise argparse.ArgumentTypeError("use 1-40 lowercase letters, digits or hyphens")
    return value


def new_run_id(scenario):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dt%H%M%S")
    return scenario + "-" + stamp + "-" + uuid.uuid4().hex[:6]


def grafana_urls(base, identifier, report):
    path = base.rstrip("/") + "/d/connect-share-comparison?"

    def link(begin, end):
        return path + urllib.parse.urlencode({"var-run_id": identifier, "from": begin, "to": end})

    urls = {"live": link("now-30m", "now")}
    if not isinstance(report, dict):
        return urls
    started = report.get("startedAtMs", 0)
    finished = report.get("measurementFinishedAtMs") or report.get("finishedAtMs", 0)
    generation_end = report.get("generationFinishedAtMs", 0)
    if started > 0 and finished >= started:
        urls["fullRun"] = link(started - 5000, finished + 5000)
    if started > 0 and generation_end - started >= 35000:
        urls["activeWindow"] = link(started - 5000, min(started + 45000, generation_end - 1))
    return urls


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("run", "suite"):
        command = commands.add_parser(name)
        command.add_argument("--url", default="http://localhost:8090")
        command.add_argument("--grafana", default="http://localhost:3000")
        command.add_argument("--rate", type=positive_float, default=100)
        command.add_argument("--seconds", type=positive_float, default=60)
        command.add_argument("--work-ms", type=int, choices=range(0, 30001), default=20,
                             metavar="0..30000")
        command.add_argument("--timeout", type=positive_float, default=900)
        command.add_argument("--results", type=Path, default=RESULTS)
    single = commands.choices["run"]
    single.add_argument("--scenario", choices=("balanced", "skewed", "mixed", "slow"),
                        default="skewed")
    single.add_argument("--tasks", type=int, choices=range(1, 10), default=9)
    single.add_argument("--run-id", type=run_id)
    single.add_argument("--min-share-parallelism", type=int, choices=range(1, 10))
    status = commands.add_parser("status")
    status.add_argument("--url", default="http://localhost:8090")
    status.add_argument("--run-id", type=run_id)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    if args.command == "status":
        state = read_report(args.url, args.run_id) if args.run_id else request_json(args.url, "GET", "/state")
        print(json.dumps(state, indent=2))
        return 0 if state is not None else 1

    cases = [(args.scenario, args.tasks, args.rate)] if args.command == "run" else [
        ("balanced", 9, args.rate / 2), ("skewed", 9, args.rate),
        ("balanced", 3, args.rate * 2), ("balanced", 6, args.rate * 2),
        ("balanced", 9, args.rate * 2),
    ]
    for scenario, tasks, rate in cases:
        identifier = args.run_id if args.command == "run" and args.run_id else new_run_id(scenario)
        payload = {"runId": identifier, "scenario": scenario,
                   "records": max(1, round(rate * args.seconds)),
                   "rate": rate, "workMs": args.work_ms, "tasks": tasks}
        code, path, evidence = run_one(
            args.url, payload, args.results, args.timeout,
            min_share_parallelism=getattr(args, "min_share_parallelism", None),
        )
        evidence["grafanaUrls"] = grafana_urls(args.grafana, identifier, evidence.get("report"))
        save_evidence(path, evidence)
        print(evidence["status"] + ": " + str(path))
        for label, address in evidence["grafanaUrls"].items():
            print(label + ": " + address)
        if code:
            return code
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as failure:
        print(str(failure), file=sys.stderr)
        sys.exit(2)
