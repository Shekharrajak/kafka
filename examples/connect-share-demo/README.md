# Kafka Connect: consumer groups versus share groups

Three input partitions, nine tasks per mode, one mirrored Grafana dashboard.
This runs the experimental KIP-1302 Connect implementation from this checkout.
Both task adapters perform identical independent document work and await the
same Kafka audit-write acknowledgement before returning from `put()`.

```text
producer -> three-partition input -> ordinary sink tasks -> classic audit topic
same input -> share sink tasks -> share audit topic
audit topics -> independent verifier -> Prometheus -> Grafana
```

The workload models external-service latency with a configurable per-record
wait, followed by deterministic text normalization. It is an I/O-bound processing
comparison, not a Kafka broker throughput benchmark. Conversion and processing
stay inside each task's worker; no external service is required.

## Start

Requirements: the checkout's Java/Gradle prerequisites, Python 3.10+, Docker
Desktop with Compose, and free ports 3000, 8080, 8083, 8084, 8090, 9090 and 19092.
Allow roughly 12 GiB of Docker memory and several GiB of free disk as a starting
budget; this is not a tested minimum. The two pools have matching resource caps,
not dedicated cores. Existing container images are pinned demo dependencies,
not recommendations for their latest releases.

From the Kafka repository root:

```bash
./gradlew :examples:connect-share-demo:stageDemo --max-workers=2
docker compose -f examples/connect-share-demo/compose.yaml up --build -d
python3 examples/connect-share-demo/scripts/demo.py status
```

| Service | Address |
|---|---|
| Mirrored dashboard | http://localhost:3000/d/connect-share-comparison |
| Kafka UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Controller | http://localhost:8090/state |
| Ordinary / share Connect | http://localhost:8083 / http://localhost:8084 |

The stack uses one broker, three workers per pool, and nine tasks per connector
by default. All published ports bind to localhost. Kafka UI is a read-only topic
and ordinary-group viewer; share membership, eligibility and lag come from the
Admin collector, independently of UI share-group support.

If another demo owns the monitoring ports, set `DEMO_GRAFANA_PORT`,
`DEMO_PROMETHEUS_PORT` and `DEMO_CONTROL_PORT` for Compose. Pass the corresponding
`--url` to the runner, validation and fault commands, `--grafana` to the runner
and dashboard validation, and `--prometheus` to validation and fault commands.

## Run the comparison

```bash
python3 examples/connect-share-demo/scripts/demo.py run --scenario balanced --rate 80 --seconds 60
python3 examples/connect-share-demo/scripts/demo.py run --scenario skewed --rate 100 --seconds 60
python3 examples/connect-share-demo/scripts/demo.py run --scenario balanced --rate 200 --seconds 60 --min-share-parallelism 4
```

These are starting rates, not promised capacities. Warm up and calibrate against
the measured single-task capacity. Repeat balanced runs with `--tasks 3`, `6`
and `9` at the same offered rate. Use `--scenario mixed` for occasional expensive
jobs and `--scenario slow` for one ten-second job that exercises renewal.
`--work-ms` defaults to 20; batch size is five records for both modes.

The producer controls partitions explicitly: balanced is one third each;
skewed is 90/5/5. It is paced independently of consumer completion. Both modes
read the same successfully published identities from an isolated topic per run.
Every job should produce one result; filtering is outside this workload.

Each command prints live, active-window and complete-run dashboard links, and
saves its report under `results/`. Use the active-window link for task activity
and the full-run results for exact completion throughput and latency. The full
run includes drain, while physical audit and connector cleanup are excluded.

Default `simple` assignment makes three members eligible per partition with nine
members. The hot partition therefore has three potential processors, versus one
ordinary owner. All-nine eligibility is an optional custom policy, selected only
between drained, verified runs:

```bash
SHARE_ASSIGNOR=demo.connect.FullSharingAssignor \
  docker compose -f examples/connect-share-demo/compose.yaml up -d broker control
```

This restarts the single broker and controller on retained storage. It is an
outage, not broker HA. Keep the policy label with every comparison; all-nine
eligibility does not guarantee equal rates or broadcast records to every task.

## Read the dashboard

Left is the ordinary consumer group; right is the share group. Matching panels
use the same range, units and scale. Select the printed run ID.

- Verified throughput counts the first correct result per published job.
- Business backlog is published identities minus verified results, by partition.
- Task participation and sampled processing concurrency are separate panels.
  Exact overlapping processing spans are also reported; they assume a shared
  clock, as in this single-host demo.
- Native offset lag and share lag are labelled separately. Missing metrics stay
  unknown, and share lag is never inferred from the start-offset frontier.
- Live latency quantiles include completed results only. Read them beside oldest
  unfinished age; final exact quantiles include every verified job.
- JVM process CPU is the JVM's reported process load, not CPU quota utilization.
  Heap is not total container memory.

## Worker recovery

During a long active run, use a second terminal:

```bash
python3 examples/connect-share-demo/scripts/fault_worker.py --mode share
```

The helper finds an actively processing worker through Prometheus and Connect,
verifies this Compose project's ownership labels, kills only that container,
and attempts to restart it. Fault evidence is saved separately. The ordinary
run verifier must still finish before calling recovery successful. Use
`--mode classic` in another run for the corresponding baseline.

This is at least once: a worker can write an output and fail before input
completion, so physical duplicate outputs are retained and reported. Success
requires complete correct coverage, no invalid/unexpected outputs, confirmed
native source progress, and an independent bounded physical output scan.

## Verify and stop

```bash
./gradlew :examples:connect-share-demo:test :examples:connect-share-demo:spotlessCheck --max-workers=2 -PmaxParallelForks=1
python3 -m unittest discover -s examples/connect-share-demo/scripts -p 'test_*.py'
python3 examples/connect-share-demo/scripts/native_smoke.py --recovery
python3 examples/connect-share-demo/scripts/validate_dashboard.py --run-id YOUR_RUN_ID
docker compose -f examples/connect-share-demo/compose.yaml down
```

Run dashboard validation before starting the next comparison. It checks every
panel query over the active and full-run windows, the provisioned dashboard and
the Grafana datasource path. It saves raw query evidence under `results/`;
browser rendering still needs a visual check.

The native smoke test starts its own broker, two Connect JVMs and nine tasks per
mode on random local ports, then stops only those processes. `--recovery` adds an
active-worker kill/restart case. It preserves logs, data and metrics under
`results/native-*`; it checks delivery and exporter integration, not the
containerized dashboard or resource comparability.

`down` preserves volumes. Retain reports and failed evidence; do not delete
volumes to hide a failed run. The controller records start reservations before
publishing and rejects duplicate IDs or unresolved previous work. It retains at
most ten runs per session. Inspect a failed run and stop its connectors before
choosing a new controller data directory; do not blindly retry generation.

This single-broker/RF1 setup does not establish EOS, broker/node HA, production
capacity or universal latency improvements. Repeat matched runs and check
shared-host interference before quoting performance results.
