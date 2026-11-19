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
FROM eclipse-temurin:21-jre-jammy
COPY kafka /opt/kafka
COPY plugin /opt/demo-plugin
COPY agent /opt/agent
COPY demo-config /opt/demo-config
COPY entrypoint.sh /opt/demo-entrypoint.sh
RUN mkdir -p /var/lib/kafka/data /data /opt/kafka/logs && \
    chmod +x /opt/kafka/bin/*.sh /opt/demo-entrypoint.sh && \
    chown -R 10001:10001 /var/lib/kafka /data /opt/kafka/logs
USER 10001
ENTRYPOINT ["/opt/demo-entrypoint.sh"]
