#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Run downstream-consumer validation for a candidate kafka-clients release.
#
# Usage:
#   ./run-validation.sh                      # default version (see scripts below)
#   ./run-validation.sh --version 4.4.0      # validate a specific version
#   ./run-validation.sh --version 4.4.0-RC1  # validate a release candidate
#
# Exit codes:
#   0 - all sample projects built and ran successfully
#   1 - at least one sample project failed
#   2 - prerequisites missing (gradle/mvn/java not on PATH)

set -euo pipefail

KAFKA_VERSION=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            KAFKA_VERSION="$2"
            shift 2
            ;;
        -h|--help)
            sed -n '20,32p' "$0"
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument '$1'" >&2
            exit 2
            ;;
    esac
done

# Verify prerequisites.
for cmd in java gradle mvn; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: '$cmd' not found on PATH" >&2
        exit 2
    fi
done

run_gradle() {
    local project="$SCRIPT_DIR/gradle-consumer"
    echo "=== gradle-consumer build + run ==="
    if [[ -n "$KAFKA_VERSION" ]]; then
        ( cd "$project" && gradle --no-daemon --quiet -PkafkaVersion="$KAFKA_VERSION" build run )
    else
        ( cd "$project" && gradle --no-daemon --quiet build run )
    fi
}

run_maven() {
    local project="$SCRIPT_DIR/maven-consumer"
    echo "=== maven-consumer build + run ==="
    if [[ -n "$KAFKA_VERSION" ]]; then
        ( cd "$project" && mvn --quiet -Dkafka.version="$KAFKA_VERSION" package exec:java )
    else
        ( cd "$project" && mvn --quiet package exec:java )
    fi
}

run_gradle
run_maven

echo "=== release-validation: ALL OK ==="
