---
paths:
  - "**/pom.xml"
---

# Flink build / test (Scala 2.13 / Maven / Flink 1.18.1)

```bash
mvn clean install -DskipTests            # default profile = azure storage connector
mvn clean install -DskipTests -Paws      # S3
mvn clean install -DskipTests -Pgcloud   # GCS   (also: -Pazure default, -Poci)
mvn clean install -DskipTests -pl <module>
mvn test -pl <module> -Dtest=<TestClass>
```

- Cloud storage profiles (`aws`/`gcloud`/`azure`/`oci`) live in `jobs-core/pom.xml` + `jobs-distribution/pom.xml` and select the connector dep — pick one that matches the deploy target.
- Job jars build to `<module>/target/<name>-1.0.0.jar`; submit via `./bin/flink run -m localhost:8081 <jar>`.
- `jobs-distribution` packages all jobs into one deployable Docker image (`docker/`, `kubernets/`).

## Running a job locally (temporary, do-not-commit edits)

**Flink deps are `provided`.** To run a job locally you must make 3 *temporary* edits (README documents them), then revert before PR:
1. add `flink-clients_${scala.version}`,
2. comment out `<scope>provided</scope>` on `flink-streaming-scala`,
3. swap the StreamTask to `StreamExecutionEnvironment.createLocalEnvironment()`.
