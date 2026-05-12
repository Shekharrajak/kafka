<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# kafka-clients release validation

Sample downstream-consumer projects that build and run against the
`kafka-clients` artefact. Used during the release process (see
[KAFKA-18965](https://issues.apache.org/jira/browse/KAFKA-18965)) to verify a
candidate release before it is staged for vote.

The samples are intentionally **standalone** Gradle and Maven projects;
they are not part of the main Kafka build. Each project consumes
`kafka-clients` exactly as a third-party application would.

## What is verified

| Goal | How |
|---|---|
| Downstream projects build with the new JAR | `gradle build` and `mvn package` succeed |
| Downstream projects run with the new JAR | `gradle run` and `mvn exec:java` execute `ClientValidation.main` and exit 0 |
| Shaded dependencies do not conflict | `ClientValidation` instantiates `KafkaProducer`, `KafkaConsumer`, `KafkaShareConsumer`, and `AdminClient`; any `NoClassDefFoundError` or `LinkageError` indicates a packaging regression |
| Licenses are present in the JAR | (Out of scope for this script; verified by the existing `verify_license.py` tool in `committer-tools/`) |

## Requirements

- Java 17+ on `PATH`
- `gradle` on `PATH`
- `mvn` on `PATH`

## Usage

Validate the default version (whatever the sample build files declare):

```bash
./run-validation.sh
```

Validate a specific staged release-candidate version:

```bash
./run-validation.sh --version 4.4.0-RC1
```

The script will fetch `org.apache.kafka:kafka-clients:<version>` from
Maven Central and the Apache staging repository, build both sample
projects, and execute the validation entry point in each.

A successful run prints `release-validation: ALL OK` and exits 0.

## Adding a new sample project

To add a new build-tool flavour (e.g. SBT, Bazel):

1. Create `clients/release-validation/<flavour>-consumer/` with the
   build configuration and a `ClientValidation` class identical in
   behaviour to the existing samples.
2. Add a `run_<flavour>` function to `run-validation.sh` and call it
   from the bottom of the script.
3. Update this README's *Requirements* and *What is verified* tables.
