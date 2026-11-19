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

"""Build the ordinary-left/share-right comparison from demo metrics."""

import json
from pathlib import Path


DATASOURCE = {"type": "prometheus", "uid": "demo-prom"}
RUN = 'run_id="$run_id"'


def target(expression, reference="A", legend="", instant=False, table=False):
    return {"refId": reference, "expr": expression, "legendFormat": legend,
            "datasource": DATASOURCE, "instant": instant,
            "range": not instant, "format": "table" if table else "time_series"}


def build():
    panels = []
    row = 0

    def add(panel, x=0, width=24, height=7):
        panel["id"] = len(panels) + 1
        panel["gridPos"] = {"x": x, "y": row, "w": width, "h": height}
        panels.append(panel)

    def text(title, content, x=0, width=24, height=2):
        add({"type": "text", "title": title,
             "options": {"mode": "markdown", "content": content}}, x, width, height)

    def chart(title, expressions, unit="short", description="", maximum=None):
        defaults = {"unit": unit, "min": 0,
                    "custom": {"drawStyle": "line", "lineWidth": 2,
                               "fillOpacity": 6, "spanNulls": False,
                               "axisCenteredZero": False, "axisLabel": "",
                               "axisBorderShow": False, "showPoints": "never"}}
        if maximum is not None:
            defaults["max"] = maximum
        return {"type": "timeseries", "title": title, "description": description,
                "datasource": DATASOURCE,
                "fieldConfig": {"defaults": defaults, "overrides": []},
                "options": {"legend": {"displayMode": "list", "placement": "bottom",
                                       "calcs": ["lastNotNull"]},
                            "tooltip": {"mode": "multi", "sort": "desc"}},
                "targets": [target(expr, chr(65 + index), legend)
                            for index, (expr, legend) in enumerate(expressions)]}

    def pair(title, expression, unit="short", description="", maximum=None,
             legend="", shared_scale=False):
        nonlocal row
        for index, mode in enumerate(("classic", "share")):
            query = expression.replace("MODE", mode)
            panel = chart(title + " - " + ("ordinary" if index == 0 else "share"),
                          [(query, legend)], unit, description, maximum)
            if shared_scale:
                combined = expression.replace('mode="MODE"', 'mode=~"classic|share"')
                ceiling = 'clamp_min(max(max_over_time((' + combined + ')[$__range:5s])) * 1.1, 1)'
                panel["targets"].append(target(ceiling, "B", instant=True, table=True))
                panel["transformations"] = [
                    {"id": "configFromData", "options": {
                        "configRefId": "B", "applyTo": {"id": "byFrameRefID", "options": "A"},
                        "mappings": [{"fieldName": "Value", "handlerKey": "max"}]}},
                    {"id": "filterByRefId", "options": {"include": "A"}},
                ]
            add(panel, index * 12, 12)
        row += 7

    text("Kafka Connect: consumer groups vs share groups",
         "Same input corpus and three input partitions. Ordinary group on the left; share group on the right. "
         "Experimental KIP-1302 Connect build; **at least once**. Empty or missing metrics remain unknown.", height=3)
    row += 3
    add({"type": "stat", "title": "Selected scenario / tasks / share policy",
         "datasource": DATASOURCE,
         "targets": [target('demo_run_info{' + RUN + '}', legend="{{scenario}} / {{tasks}} tasks / {{share_policy}}", instant=True)],
         "options": {"textMode": "name", "colorMode": "none", "graphMode": "none"},
         "fieldConfig": {"defaults": {"noValue": "No selected run"}, "overrides": []}}, 0, 16, 3)
    add({"type": "stat", "title": "Run phase",
         "datasource": DATASOURCE,
         "targets": [target('demo_run_status{' + RUN + '} == 1', legend="{{status}}", instant=True)],
         "options": {"textMode": "name", "colorMode": "none", "graphMode": "none"},
         "fieldConfig": {"defaults": {"noValue": "Unknown"}, "overrides": []}}, 16, 8, 3)
    row += 3
    add(chart("Confirmed input publication by partition", [
        ('sum by (partition) (rate(demo_published_total{' + RUN + '}[30s]))', "P{{partition}}")
    ], "ops", "Successful producer acknowledgements for unique input jobs. This is offered load, not sink completion."))
    row += 7
    text("Ordinary consumer group", "Exclusive partition readers", 0, 12)
    text("Share group", "Distinct acquired records; sharing follows the selected broker policy", 12, 12)
    row += 2

    for index, mode in enumerate(("classic", "share")):
        selector = RUN + ',mode="' + mode + '"'
        summaries = [
            ("Verified / sec", 'sum(rate(demo_verified_total{' + selector + '}[30s]))', "ops"),
            ("Unfinished", 'sum(demo_unfinished{' + selector + '})', "short"),
            ("E2E p95", 'histogram_quantile(0.95,sum by (le) (rate(demo_e2e_seconds_bucket{' + selector + '}[30s])))', "s"),
            ("Members", 'demo_members{' + selector + '}', "short"),
        ]
        for column, (title, expression, unit) in enumerate(summaries):
            add({"type": "stat", "title": title,
                 "datasource": DATASOURCE, "targets": [target(expression, instant=True)],
                 "options": {"textMode": "value", "colorMode": "none", "graphMode": "none",
                             "reduceOptions": {"calcs": ["lastNotNull"], "values": False}},
                 "fieldConfig": {"defaults": {"unit": unit, "noValue": "Unknown"}, "overrides": []}},
                index * 12 + column * 3, 3, 4)
    row += 4

    verified = 'sum(rate(demo_verified_total{' + RUN + ',mode="MODE"}[30s]))'
    pair("Verified unique results / second", verified, "ops",
         "First correct output for each job ID, independently observed at the destination. Duplicates do not increase useful throughput.",
         shared_scale=True)
    pair("Tasks completing useful work in 30 seconds",
         'count(sum by (task) (rate(demo_verified_total{' + RUN + ',mode="MODE"}[30s])) > 0)',
         description="Participation over a window; this does not prove those tasks ran simultaneously.", maximum=9)
    pair("Sampled concurrent processing tasks",
         'sum(demo_task_processing{' + RUN + ',mode="MODE"} and on (job, instance) (up{job="connect-tasks"} == 1))',
         description="Scraped work-in-progress gauges, including the awaited destination write. Failed scrape targets are excluded. "
                     "Use the full-run processing-span overlap report for a stronger concurrency check.", maximum=9)
    pair("Verified result rate by task",
         'sum by (task) (rate(demo_verified_total{' + RUN + ',mode="MODE"}[30s]))', "ops",
         "Original task IDs from verified audit records. No completed output means no series; this does not by itself mean a task failed.",
         legend="Task {{task}}", shared_scale=True)

    for index, mode in enumerate(("classic", "share")):
        add({"type": "table", "title": "Task x input partition - " + ("ordinary" if index == 0 else "share"),
             "description": "Verified unique results over the last 30 seconds. Cell shading shows actual work from each original input partition. "
                            "Blank cells mean no observed series, not fabricated zeros.",
             "datasource": DATASOURCE,
             "targets": [target('sum by (task, partition) (increase(demo_verified_total{' + RUN + ',mode="' + mode + '"}[30s]))',
                                instant=True, table=True)],
             "transformations": [{"id": "groupingToMatrix", "options": {
                 "columnField": "partition", "rowField": "task", "valueField": "Value", "emptyValue": "null"}}],
             "fieldConfig": {"defaults": {"decimals": 1, "noValue": "",
                                           "color": {"mode": "continuous-BlYlRd"},
                                           "custom": {"cellOptions": {"type": "color-background", "mode": "gradient"}}},
                             "overrides": [{"matcher": {"id": "byType", "options": "string"},
                                            "properties": [{"id": "custom.cellOptions", "value": {"type": "auto"}}]}]},
             "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}}, index * 12, 12, 8)
    row += 8
    pair("Who completes hot P0 records?",
         'sum by (task) (rate(demo_verified_total{' + RUN + ',mode="MODE",partition="0"}[30s]))', "ops",
         "Actual P0 completion by task, separate from eligibility to fetch P0.", legend="Task {{task}}", shared_scale=True)
    pair("Verified result rate by input partition",
         'sum by (partition) (rate(demo_verified_total{' + RUN + ',mode="MODE"}[30s]))', "ops",
         legend="P{{partition}}", shared_scale=True)
    pair("Unfinished jobs by input partition",
         'sum by (partition) (demo_unfinished{' + RUN + ',mode="MODE"})',
         description="Successfully published unique jobs minus correct unique outputs. This is comparable business backlog, not native source lag.",
         legend="P{{partition}}", shared_scale=True)

    for index, mode in enumerate(("classic", "share")):
        expressions = [
            ('histogram_quantile(' + str(quantile) + ',sum by (le) (rate(demo_e2e_seconds_bucket{' + RUN + ',mode="' + mode + '"}[30s])))', name)
            for quantile, name in ((0.50, "p50"), (0.95, "p95"), (0.99, "p99"))
        ]
        panel = chart("End-to-end completion latency - " + ("ordinary" if index == 0 else "share"), expressions, "s",
                      "Input publication timestamp to first correct output observation. Rolling histogram estimates include completed jobs only. "
                      "Read with unfinished count and oldest unfinished age; exact full-run values are in the saved report.")
        maximum_latency = 'clamp_min(max(max_over_time((histogram_quantile(0.99,sum by (mode,le) (rate(demo_e2e_seconds_bucket{' + RUN + '}[30s]))))[$__range:5s])) * 1.1, 0.1)'
        panel["targets"].append(target(maximum_latency, "D", instant=True, table=True))
        panel["transformations"] = [
            {"id": "configFromData", "options": {"configRefId": "D",
                "applyTo": {"id": "byFrameRefID", "options": "A|B|C"},
                "mappings": [{"fieldName": "Value", "handlerKey": "max"}]}},
            {"id": "filterByRefId", "options": {"include": "A|B|C"}},
        ]
        add(panel, index * 12, 12)
    row += 7
    pair("Oldest unfinished job age",
         'max(demo_oldest_unfinished_seconds{' + RUN + ',mode="MODE"})', "s",
         "Age of the oldest successfully published job without a verified correct output. This exposes work absent from completion histograms.",
         shared_scale=True)

    for index, mode in enumerate(("classic", "share")):
        title = "Committed-offset lag" if index == 0 else "Native share lag"
        add(chart(title, [('demo_source_lag{' + RUN + ',mode="' + mode + '"}', "P{{partition}}")],
                  description="Native Admin API measurement. These source-progress measures have different semantics; compare business backlog above. "
                              "Unavailable share lag is omitted, never replaced with zero."), index * 12, 12)
    row += 7
    pair("Joined group members", 'demo_members{' + RUN + ',mode="MODE"}',
         description="Membership does not prove partition assignment or processing activity.", maximum=9)
    pair("Eligible members per partition",
         'demo_partition_eligible_members{' + RUN + ',mode="MODE"}',
         description="Ordinary assignments are exclusive. Share eligibility follows the broker assignor; default simple gives three per partition for 9 members / 3 partitions.",
         maximum=9, legend="P{{partition}}")

    add(chart("Share acquisitions in flight", [
        ('kafka_share_inflight_records{group="demo-share-$run_id"}', "P{{partition}}")
    ], description="Broker acquisition state. In-flight records are not the same as executing jobs or all unfinished work."), 0, 12)
    add(chart("Share acquisition-lock timeouts / second", [
        ('rate(kafka_share_lock_timeouts_total{group="demo-share-$run_id"}[30s])', "P{{partition}}")
    ], "ops", "Broker record-lease expirations for this share group. Read alongside renewals and processing deadlines."), 12, 12)
    row += 7
    add(chart("Share acknowledgements / second - broker-wide", [
        ('sum by (ack_type) (rate(kafka_share_acknowledgements_total[30s]))', "{{ack_type}}")
    ], "ops", "Broker-wide Accept/Release/Reject/Renew activity, not a per-task completion count. Renewal is not useful output."), 0, 12)
    add(chart("Admin collector health", [
        ('demo_admin_ok{' + RUN + '}', "{{mode}}")
    ], description="One means the latest native group query succeeded. Missing or failed native data is not interpreted as zero lag.", maximum=1), 12, 12)
    row += 7

    pair("JVM process CPU fraction", 'demo_jvm_process_cpu_ratio{mode="MODE"}', "percentunit",
         "Raw OperatingSystem ProcessCpuLoad fraction. This is not CPU divided by the pod quota.", maximum=1, legend="{{worker}}")
    pair("JVM heap usage / maximum",
         'demo_jvm_heap_used_bytes{mode="MODE"} / demo_jvm_heap_max_bytes{mode="MODE"}', "percentunit",
         "JVM heap only. Native memory and container page cache are not included.", maximum=1, legend="{{worker}}")
    pair("Duplicate physical output occurrences", 'demo_duplicates_total{' + RUN + ',mode="MODE"}',
         description="Additional physical output occurrences of an input identity. At-least-once recovery can produce duplicates; they remain visible.", shared_scale=True)
    pair("Invalid / unexpected output occurrences", 'demo_invalid_total{' + RUN + ',mode="MODE"}',
         description="Wrong payload, result or identity. A successful run must not hide these behind deduplication.", shared_scale=True)
    pair("Task processing failures / second",
         'sum(rate(demo_task_failures_total{' + RUN + ',mode="MODE"}[30s]))', "ops",
         "Plugin failures. Retry/write attempts are separate from verified unique completion.", shared_scale=True)

    return {
        "uid": "connect-share-comparison", "title": "Kafka Connect - consumer group vs share group",
        "description": "Finite, verified at-least-once KIP-1302 comparison. Ordinary left, share right. Never substitutes fetched/ACK counts for useful completed work.",
        "tags": ["kafka", "connect", "share-groups", "kip-1302"], "schemaVersion": 39,
        "version": 1, "editable": True, "refresh": "5s", "timezone": "browser",
        "time": {"from": "now-15m", "to": "now"},
        "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m"]},
        "templating": {"list": [{"name": "run_id", "label": "Run", "type": "query",
            "datasource": DATASOURCE, "definition": "label_values(demo_published_total, run_id)",
            "query": {"query": "label_values(demo_published_total, run_id)", "refId": "RunVariable"},
            "refresh": 1, "sort": 2, "multi": False, "includeAll": False}]},
        "annotations": {"list": []},
        "links": [{"title": "Kafka UI", "url": "http://localhost:8080", "targetBlank": True},
                  {"title": "Controller state", "url": "http://localhost:8090/state", "targetBlank": True},
                  {"title": "Prometheus targets", "url": "http://localhost:9090/targets", "targetBlank": True}],
        "panels": panels,
    }


if __name__ == "__main__":
    path = Path(__file__).with_name("dashboard.json")
    path.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print(path)
