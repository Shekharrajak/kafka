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

"""Kill and restart one actively processing worker in this Compose demo."""

import argparse
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.parse
import uuid

from demo import RESULTS, request_json, save_evidence


PROJECT = "kafka-connect-share-demo"
ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "compose.yaml"
ACTIVE = {"GENERATING", "DRAINING"}


def choose_worker(mode, task_status, metrics):
    allowed = {mode + "-" + str(index) for index in (1, 2, 3)}
    running = {
        task.get("worker_id", "").split(":")[0]
        for task in task_status.get("tasks", []) if task.get("state") == "RUNNING"
    }
    candidates = []
    for sample in metrics:
        worker = sample.get("metric", {}).get("worker")
        try:
            value = float(sample["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        if worker in allowed & running and math.isfinite(value) and value > 0:
            candidates.append((value, worker))
    if not candidates:
        raise ValueError("No matching worker has a RUNNING task and fresh positive processing activity")
    return max(candidates)[1]


def verify_labels(worker, labels):
    if not re.fullmatch(r"(?:classic|share)-[123]", worker):
        raise ValueError("Only the six Connect worker services can be targeted")
    configured = labels.get("com.docker.compose.project.config_files", "").split(",")
    if (labels.get("com.docker.compose.project") != PROJECT
            or labels.get("com.docker.compose.service") != worker
            or str(COMPOSE) not in configured):
        raise ValueError("Container labels do not identify this demo's exact Compose project and worker")


def command(arguments, timeout=30):
    return subprocess.run(arguments, check=True, text=True, capture_output=True,
                          timeout=timeout).stdout.strip()


def current_run(url, expected=None):
    current = request_json(url, "GET", "/state").get("current")
    if (not isinstance(current, dict) or current.get("status") not in ACTIVE
            or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,39}", current.get("runId", ""))
            or (expected is not None and current["runId"] != expected)):
        raise ValueError("The expected run must still be generating or draining")
    return current


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("classic", "share"), required=True)
    parser.add_argument("--url", default="http://localhost:8090")
    parser.add_argument("--connect", help="Connect REST URL; defaults to localhost:8083 or :8084")
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--down-seconds", type=int, choices=range(1, 61), default=5, metavar="1..60")
    parser.add_argument("--results", type=Path, default=RESULTS)
    args = parser.parse_args(argv)
    run = current_run(args.url)
    identifier = run["runId"]
    connect = args.connect or ("http://localhost:8083" if args.mode == "classic" else "http://localhost:8084")
    status = request_json(connect, "GET", "/connectors/demo-" + args.mode + "-" + identifier + "/status")
    selector = 'demo_task_processing{run_id="' + identifier + '",mode="' + args.mode + '"}'
    query = ('sum by (worker) (' + selector
             + ' and on (job, instance) (up{job="connect-tasks"} == 1)'
             + ' and (time() - timestamp(' + selector + ') < 15))')
    response = request_json(args.prometheus, "GET", "/api/v1/query?" + urllib.parse.urlencode({"query": query}))
    if response.get("status") != "success" or response.get("data", {}).get("resultType") != "vector":
        raise ValueError("Prometheus did not return a successful instant vector")
    worker = choose_worker(args.mode, status, response["data"]["result"])
    container = command(["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE), "ps", "-q", worker])
    if not re.fullmatch(r"[a-f0-9]{12,64}", container):
        raise ValueError("Expected exactly one running worker container")
    labels = json.loads(command(["docker", "inspect", "--format", "{{json .Config.Labels}}", container]))
    verify_labels(worker, labels)
    current_run(args.url, identifier)

    args.results.mkdir(parents=True, exist_ok=True)
    path = args.results / (identifier + "-fault-" + uuid.uuid4().hex[:8] + ".json")
    evidence = {"runId": identifier, "mode": args.mode, "worker": worker,
                "container": container, "downSeconds": args.down_seconds,
                "status": "KILL_REQUESTED", "requestedAtMs": round(time.time() * 1000),
                "processingSamples": response["data"]["result"]}
    with path.open("x", encoding="utf-8") as output:
        json.dump(evidence, output)
    save_evidence(path, evidence)
    code = 0
    try:
        command(["docker", "kill", "--signal", "KILL", container])
        evidence["killedAtMs"] = round(time.time() * 1000)
        evidence["status"] = "WORKER_KILLED"
        save_evidence(path, evidence)
        time.sleep(args.down_seconds)
    except BaseException as failure:
        evidence["status"] = "FAULT_OUTCOME_UNKNOWN"
        evidence["error"] = str(failure)
        code = 130 if isinstance(failure, KeyboardInterrupt) else 1
    finally:
        try:
            command(["docker", "start", container])
            evidence["restartRequestedAtMs"] = round(time.time() * 1000)
            if code == 0:
                evidence["status"] = "WORKER_RESTART_REQUESTED"
        except (OSError, subprocess.SubprocessError) as failure:
            evidence["status"] = "RESTART_FAILED"
            evidence["restartError"] = str(failure)
            code = 1
        evidence["note"] = "Worker restart is not a recovery verdict; inspect the existing run's final verification report."
        save_evidence(path, evidence)
    print(evidence["status"] + ": " + str(path))
    return code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as failure:
        print(str(failure), file=sys.stderr)
        sys.exit(2)
