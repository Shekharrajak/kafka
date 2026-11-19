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

"""Verify staged Kafka/Connect processes locally; keep all logs and storage."""

import argparse
import base64
import datetime
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[1]
METRIC = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+(\S+)(?:\s+\S+)?$")


def metric_samples(body):
    for line in body.decode("utf-8").splitlines():
        match = METRIC.fullmatch(line)
        if match is not None:
            try:
                value = float(match[3])
            except ValueError:
                continue
            if math.isfinite(value):
                yield match[1], match[2] or "", value, line


def metric_requirements(jmx):
    plugin = ("demo_task_processing", "demo_task_processed_total", "demo_task_failures_total")
    required = {"control": ("demo_published_total", "demo_verified_total", "demo_unfinished",
                            "demo_e2e_seconds_count", "demo_members", "demo_partition_eligible_members",
                            "demo_source_lag"), "classic": plugin, "share": plugin}
    if jmx:
        jvm = ("demo_jvm_process_cpu_ratio", "demo_jvm_heap_used_bytes", "demo_jvm_heap_max_bytes", "jmx_scrape_error")
        required.update({name + "-jmx": jvm for name in ("broker", "classic", "share")})
        required["broker-jmx"] += ("kafka_share_inflight_records", "kafka_share_lock_timeouts_total",
                                    "kafka_share_acknowledgements_total")
    return required


def validate_metrics(required, observed):
    missing = [f"{endpoint}/{family}" for endpoint, families in required.items()
               for family in families if family not in observed[endpoint]]
    if missing:
        raise RuntimeError("Missing finite mapped metric samples: " + ", ".join(missing))
    for endpoint in ("classic", "share"):
        if observed[endpoint]["demo_task_processed_total"]["max"] <= 0:
            raise RuntimeError(f"No confirmed destination-write metric from {endpoint}")
    for endpoint, families in observed.items():
        if endpoint.endswith("-jmx") and families["jmx_scrape_error"]["max"] != 0:
            raise RuntimeError(f"JMX exporter reported a scrape error: {endpoint}")
    if "broker-jmx" in required and not observed["broker-jmx"]["kafka_share_acknowledgements_total"].get("acceptObserved"):
        raise RuntimeError("No positive mapped share ACCEPT acknowledgement counter")


def ports(count):
    sockets = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            sockets.append(listener)
        return [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


def properties(source, target, overrides):
    text = source.read_text(encoding="utf-8")
    target.write_text(text + "\n" + "\n".join(f"{key}={value}" for key, value in overrides.items())
                      + "\n", encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", type=Path, default=ROOT / "build/image")
    parser.add_argument("--results", type=Path, default=ROOT / "results")
    parser.add_argument("--no-jmx", action="store_true", help="Explicitly skip exporter verification")
    parser.add_argument("--recovery", action="store_true", help="Also kill and restart this smoke's share worker during processing")
    args = parser.parse_args(argv)
    stage = args.stage.resolve()
    kafka = stage / "kafka"
    for required in (kafka / "bin/kafka-server-start.sh", stage / "plugin"):
        if not required.exists():
            parser.error(f"Missing {required}; run :examples:connect-share-demo:stageDemo first")
    if not args.no_jmx and not (stage / "agent/jmx-agent.jar").is_file():
        parser.error("Missing staged JMX agent; stage it or explicitly use --no-jmx")

    identifier = "native-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    output = args.results.resolve() / identifier
    output.mkdir(parents=True, exist_ok=False)
    assigned = dict(zip(("broker", "controller", "classic", "share", "control",
                         "classic_metrics", "share_metrics", "broker_jmx", "classic_jmx", "share_jmx"), ports(10)))
    bootstrap = f"127.0.0.1:{assigned['broker']}"
    metadata = {"directory": str(output), "ports": assigned, "stage": str(stage),
                "topology": "one broker; one Connect JVM per mode; nine tasks per mode",
                "jmxEnabled": not args.no_jmx, "status": "STARTING"}
    processes = []
    logs = []
    collecting = threading.Event()
    collector = None
    fault_thread = None
    fault_errors = []

    def interrupt(_signal, _frame):
        raise KeyboardInterrupt

    previous_term = signal.signal(signal.SIGTERM, interrupt)

    def save():
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    def environment(name, heap):
        env = dict(os.environ, LOG_DIR=str(output / f"{name}-logs"), KAFKA_HEAP_OPTS=heap)
        env.pop("KAFKA_OPTS", None)
        env.pop("JMX_PORT", None)
        if not args.no_jmx and name in ("broker", "classic", "share"):
            env["KAFKA_OPTS"] = (f"-javaagent:{stage / 'agent/jmx-agent.jar'}="
                                 f"{assigned[name + '_jmx']}:{stage / 'demo-config/jmx.yaml'}")
        return env

    def launch(name, command, env):
        log = (output / f"{name}.log").open("ab")
        logs.append(log)
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        processes.append((name, process))
        return process

    def healthy():
        for name, process in processes:
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited with {process.returncode}; see {output / (name + '.log')}")

    def wait_url(url):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if collecting.is_set():
                raise InterruptedError("Native smoke is shutting down")
            healthy()
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    if response.status == 200:
                        return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.25)
        raise TimeoutError(f"Readiness timed out: {url}")

    try:
        save()
        server_config = output / "server.properties"
        properties(ROOT / "config/server.properties", server_config, {
            "controller.quorum.bootstrap.servers": f"127.0.0.1:{assigned['controller']}",
            "listeners": f"PLAINTEXT://127.0.0.1:{assigned['broker']},CONTROLLER://127.0.0.1:{assigned['controller']}",
            "advertised.listeners": f"PLAINTEXT://{bootstrap}",
            "listener.security.protocol.map": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            "log.dirs": output / "broker-data",
        })
        cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode("ascii").rstrip("=")
        with (output / "format.log").open("wb") as log:
            subprocess.run(["bash", str(kafka / "bin/kafka-storage.sh"), "format", "--standalone",
                            "--cluster-id=" + cluster_id, "-c", str(server_config)],
                           env=environment("format", "-Xms128m -Xmx512m"), stdout=log,
                           stderr=subprocess.STDOUT, timeout=60, check=True)
        launch("broker", ["bash", str(kafka / "bin/kafka-server-start.sh"), str(server_config)],
               environment("broker", "-Xms256m -Xmx512m"))
        for mode in ("classic", "share"):
            config = output / f"connect-{mode}.properties"
            prefix = identifier + "-" + mode
            properties(ROOT / "config/connect.properties", config, {
                "bootstrap.servers": bootstrap, "group.id": prefix,
                "config.storage.topic": prefix + "-config", "offset.storage.topic": prefix + "-offsets",
                "status.storage.topic": prefix + "-status", "plugin.path": stage / "plugin",
                "listeners": f"http://127.0.0.1:{assigned[mode]}",
                "rest.advertised.host.name": "127.0.0.1", "rest.advertised.port": assigned[mode],
            })
            env = environment(mode, "-Xms128m -Xmx384m")
            env["DEMO_METRICS_PORT"] = str(assigned[mode + "_metrics"])
            launch(mode, ["bash", str(kafka / "bin/connect-distributed.sh"), str(config)], env)
        for mode in ("classic", "share"):
            wait_url(f"http://127.0.0.1:{assigned[mode]}/connectors")
        env = environment("control", "-Xms128m -Xmx512m")
        env.update(BOOTSTRAP_SERVERS=bootstrap, CLASSIC_CONNECT=f"http://127.0.0.1:{assigned['classic']}",
                   SHARE_CONNECT=f"http://127.0.0.1:{assigned['share']}", DATA_DIR=str(output / "control-data"),
                   PORT=str(assigned["control"]))
        java = str(Path(env["JAVA_HOME"]) / "bin/java") if env.get("JAVA_HOME") else "java"
        launch("control", [java, "-Xms128m", "-Xmx512m", "-cp", f"{kafka}/libs/*:{stage}/plugin/*",
                           "demo.connect.DemoControl"], env)
        control = f"http://127.0.0.1:{assigned['control']}"
        wait_url(control + "/health")
        endpoints = {"control": control + "/metrics"}
        endpoints.update({mode: f"http://127.0.0.1:{assigned[mode + '_metrics']}/metrics"
                          for mode in ("classic", "share")})
        if not args.no_jmx:
            endpoints.update({name + "-jmx": f"http://127.0.0.1:{assigned[name + '_jmx']}/metrics"
                              for name in ("broker", "classic", "share")})
        required = metric_requirements(not args.no_jmx)
        observed = {name: {} for name in endpoints}
        coverage = {name: (-1, -1) for name in endpoints}

        def snapshot_metrics():
            while not collecting.is_set():
                for name, url in endpoints.items():
                    try:
                        with urllib.request.urlopen(url, timeout=1) as response:
                            body = response.read()
                        if body:
                            samples = list(metric_samples(body))
                            for family, labels, value, line in samples:
                                if family in required[name]:
                                    info = observed[name].setdefault(family, {"min": value, "max": value, "samples": 0, "example": line})
                                    info.update(min=min(info["min"], value), max=max(info["max"], value), samples=info["samples"] + 1)
                                    if family == "kafka_share_acknowledgements_total" and 'ack_type="Accept"' in labels and value > 0:
                                        info["acceptObserved"] = True
                            families = {family for family, _, _, _ in samples}
                            positive = {family for family, _, value, _ in samples if value > 0}
                            score = (len(families.intersection(required[name])), len(positive.intersection(required[name])))
                            if score > coverage[name]:
                                target = output / f"{name}.prom"
                                temporary = target.with_suffix(".prom.tmp")
                                temporary.write_bytes(body)
                                temporary.replace(target)
                                coverage[name] = score
                    except (OSError, urllib.error.URLError):
                        pass
                collecting.wait(0.5)

        collector = threading.Thread(target=snapshot_metrics, name="native-metrics", daemon=True)
        collector.start()

        def recover_worker(run_id):
            try:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline and not collecting.is_set():
                    with urllib.request.urlopen(control + "/state", timeout=2) as response:
                        current = json.load(response).get("current") or {}
                    try:
                        with urllib.request.urlopen(endpoints["share"], timeout=2) as response:
                            samples = list(metric_samples(response.read()))
                    except OSError:
                        samples = []
                    active = any(family == "demo_task_processing" and value > 0 and f'run_id="{run_id}"' in labels
                                 for family, labels, value, _ in samples)
                    if current.get("runId") == run_id and current.get("status") == "GENERATING" and active:
                        break
                    collecting.wait(0.1)
                else:
                    raise RuntimeError("Recovery could not observe active share processing during generation")
                name, worker = next((name, process) for name, process in processes if name == "share")
                metadata["fault"] = {"runId": run_id, "oldPid": worker.pid, "status": "KILLING",
                                     "killedAtMs": round(time.time() * 1000)}
                save()
                os.killpg(worker.pid, signal.SIGKILL)
                worker.wait(timeout=10)
                processes.remove((name, worker))
                if collecting.wait(2):
                    return
                env = environment("share", "-Xms128m -Xmx384m")
                env["DEMO_METRICS_PORT"] = str(assigned["share_metrics"])
                replacement = launch("share", ["bash", str(kafka / "bin/connect-distributed.sh"),
                                                str(output / "connect-share.properties")], env)
                metadata["fault"].update(newPid=replacement.pid, status="RESTARTING")
                save()
                wait_url(f"http://127.0.0.1:{assigned['share']}/connectors")
                metadata["fault"].update(status="WORKER_RESTARTED", readyAtMs=round(time.time() * 1000))
                save()
            except Exception as error:
                fault_errors.append(str(error))
                metadata.setdefault("fault", {}).update(status="FAILED", error=str(error))
                save()

        cases = [("balanced", 60, 1, 10, None), ("balanced", 180, 3, 30, 4), ("skewed", 80, 3, 30, None)]
        if args.recovery:
            cases.append(("balanced", 100, 12, 30, None))
        for index, (scenario, rate, seconds, work, minimum) in enumerate(cases):
            healthy()
            command = [sys.executable, str(ROOT / "scripts/demo.py"), "run", "--scenario", scenario,
                       "--rate", str(rate), "--seconds", str(seconds), "--work-ms", str(work),
                       "--tasks", "9", "--results", str(output / "reports"), "--url", control, "--timeout", "240"]
            if minimum is not None:
                command += ["--min-share-parallelism", str(minimum)]
            recovery = args.recovery and index == 3
            if recovery:
                run_id = "recovery-" + uuid.uuid4().hex[:16]
                command += ["--run-id", run_id]
                fault_thread = threading.Thread(target=recover_worker, args=(run_id,), name="native-worker-recovery", daemon=True)
                fault_thread.start()
            subprocess.run(command, timeout=270, check=True)
            if recovery:
                fault_thread.join(timeout=10)
                if fault_thread.is_alive() or fault_errors:
                    raise RuntimeError("Recovery fault did not finish: " + "; ".join(fault_errors))
                metadata["fault"]["status"] = "RECOVERY_VERIFIED"
            report_path = max((output / "reports").glob("*.json"), key=lambda path: path.stat().st_mtime_ns)
            report = json.loads(report_path.read_text(encoding="utf-8"))["report"]
            recap = {mode: {key: values[key] for key in ("unique", "duplicates", "unfinished", "maxConcurrentTasks")}
                     for mode, values in report["modes"].items()}
            metadata.setdefault("runs", []).append({"report": str(report_path), "scenario": scenario, "modes": recap})
            print(json.dumps({"scenario": scenario, "modes": recap}))
        collecting.set()
        collector.join(timeout=10)
        if collector.is_alive():
            raise RuntimeError("Metrics collector did not stop")
        metadata["metricsValidation"] = observed
        validate_metrics(required, observed)
        metadata["status"] = "PASSED"
        print(f"Finite correctness and parallelism smoke passed: {output}")
        print("These short local runs are not throughput or latency benchmark results.")
        return 0
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        metadata.update(status="FAILED", error=str(error))
        print(f"Native smoke failed; evidence retained at {output}: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        metadata["status"] = "INTERRUPTED"
        return 130
    finally:
        collecting.set()
        if fault_thread is not None:
            fault_thread.join(timeout=10)
        if collector is not None:
            collector.join(timeout=10)
        for _, process in reversed(processes):
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for _, process in reversed(processes):
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=10)
        for log in logs:
            log.close()
        signal.signal(signal.SIGTERM, previous_term)
        save()


if __name__ == "__main__":
    sys.exit(main())
