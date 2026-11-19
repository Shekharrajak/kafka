#!/usr/bin/env bash
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
set -euo pipefail

case "${1:?Expected broker, connect or control}" in
  broker)
    cluster_id=GFi1mE4wT9a3hNvqQ0cQWg
    if [[ -f /var/lib/kafka/data/meta.properties ]]; then
      if ! grep -qx "cluster.id=$cluster_id" /var/lib/kafka/data/meta.properties; then
        echo 'Refusing to use storage from another cluster' >&2
        exit 1
      fi
    else
      /opt/kafka/bin/kafka-storage.sh format --standalone -t "$cluster_id" -c /opt/demo-config/server.properties
    fi
    export KAFKA_OPTS="-javaagent:/opt/agent/jmx-agent.jar=9404:/opt/demo-config/jmx.yaml"
    exec /opt/kafka/bin/kafka-server-start.sh /opt/demo-config/server.properties --override "group.share.assignors=${SHARE_ASSIGNOR:-simple}"
    ;;
  connect)
    cp /opt/demo-config/connect.properties /tmp/connect.properties
    cat >> /tmp/connect.properties <<EOF
group.id=demo-connect-${MODE:?}
config.storage.topic=demo-connect-${MODE}-config
offset.storage.topic=demo-connect-${MODE}-offsets
status.storage.topic=demo-connect-${MODE}-status
rest.advertised.host.name=${WORKER_HOST:?}
rest.advertised.port=8083
EOF
    export KAFKA_OPTS="-javaagent:/opt/agent/jmx-agent.jar=9404:/opt/demo-config/jmx.yaml"
    exec /opt/kafka/bin/connect-distributed.sh /tmp/connect.properties
    ;;
  control)
    exec java -Xms128m -Xmx768m -cp '/opt/kafka/libs/*:/opt/demo-plugin/*' demo.connect.DemoControl
    ;;
  *) echo 'Expected broker, connect or control' >&2; exit 1 ;;
esac
