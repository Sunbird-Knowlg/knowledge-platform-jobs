# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

**Build entire project (skip tests):**
```shell
mvn clean install -DskipTests
```

**Build with cloud storage dependencies (required for most modules):**
```shell
mvn clean install -DskipTests \
  -DCLOUD_STORE_GROUP_ID=<group_id> \
  -DCLOUD_STORE_ARTIFACT_ID=<artifact_id> \
  -DCLOUD_STORE_VERSION=<version>
```

**Build only `jobs-core` and `publish-core` (faster iteration):**
```shell
mvn clean install -DskipTests -P knowlg-core
```

**Build a specific module:**
```shell
cd <module-name>
mvn clean install -DskipTests
```

## Running Tests

**Run tests for all modules:**
```shell
mvn clean test
```

**Run tests for a single module:**
```shell
cd <module-name>
mvn clean test
```

**Run a single test class:**
```shell
cd <module-name>
mvn test -Dtest=ClassName
```

**Run tests with JaCoCo coverage report:**
```shell
cd <module-name>
mvn clean org.jacoco:jacoco-maven-plugin:0.8.8:prepare-agent test org.jacoco:jacoco-maven-plugin:0.8.8:report surefire-report:report
```

Tests use ScalaTest (`FlatSpec` + `Matchers`) with embedded Kafka (`scalatest-embedded-kafka`) and Mockito. Test specs follow the naming pattern `*Spec.scala` or `*TestSpec.scala`.

## Local Development (IntelliJ Debug Mode)

To run a job locally from IntelliJ (not via Flink cluster):

1. In the job's `pom.xml`, add `flink-clients` dependency and remove `<scope>provided</scope>` from `flink-streaming-scala`.
2. In the `StreamTask` file, replace `FlinkUtil.getExecutionContext(config)` with `StreamExecutionEnvironment.createLocalEnvironment()`.
3. Set cloud storage environment variables (see `README.md`).
4. Start all Docker services (Neo4j, Cassandra, Redis, Kafka/Zookeeper, Elasticsearch).
5. Run/debug the `StreamTask` object's `main` method.

## Architecture

This project contains Apache Flink stream-processing jobs for the Sunbird Knowledge Platform. All jobs consume from Kafka, process events, and write results back to Kafka or external stores (Cassandra, Elasticsearch, Neo4j/JanusGraph).

### Multi-module Maven Structure

- **`jobs-core`** — Shared library depended on by all jobs. Contains:
  - `BaseJobConfig` — typed config wrapper over Typesafe Config
  - `BaseProcessFunction` / `BaseProcessKeyedFunction` / `WindowBaseProcessFunction` — abstract Flink `ProcessFunction` wrappers with built-in metrics tracking
  - `FlinkKafkaConnector` — Kafka source/sink factory
  - `FlinkUtil` — `StreamExecutionEnvironment` factory (handles checkpointing, state backend)
  - `util/` — `CassandraUtil`, `ElasticSearchUtil`, `CloudStorageUtil`, `HttpUtil`, `JSONUtil`, `ScalaJsonUtil`, etc.
  - `cache/` — `DataCache` (Redis-backed), `RedisConnect`
  - `dedup/` — `DeDupEngine` for event deduplication

- **`publish-pipeline/`** — Content/collection publish jobs:
  - `publish-core` — Shared publishing library: `ObjectReader`, `ObjectEnrichment`, `ObjectUpdater`, `EcarGenerator`, `ObjectBundle`, etc.
  - `knowlg-publish` — Main publish job. Routes events via `PublishEventRouter` to side-output streams, then processes via `ContentPublishFunction`, `CollectionPublishFunction`, `QuestionPublishFunction`, `QuestionSetPublishFunction`
  - `live-node-publisher` — Publishes live (already-published) nodes

- **`search-indexer`** — Indexes content into Elasticsearch; handles composite search and direct ES indexing

- **`transaction-event-processor`** — Processes Neo4j/graph transaction events and routes them to downstream consumers

- **`asset-enrichment`** — Enriches image/video assets (thumbnail generation, metadata extraction); requires ImageMagick

- **`qrcode-image-generator`** — Generates QR code images and uploads to cloud storage

- **`dialcode-context-updater`** — Updates DIAL code context in the graph

- **`cassandra-data-migration`** — Batch Flink job for Cassandra data migrations

- **`video-stream-generator`** — Triggers video streaming pipeline for published video content

- **`jobs-distribution`** — Packaging module. Builds a tar.gz of all job JARs for deployment. Also contains the `Dockerfile` (based on `flink:1.15.2-scala_2.12-java11`) used in CI to produce the final image.

### Job Anatomy

Each job follows the same pattern:
1. **`*Config.scala`** — Extends `BaseJobConfig`; declares Kafka topic names, output tags, and job-specific settings from a HOCON `.conf` file in `src/main/resources/`
2. **`*StreamTask.scala`** — Wires up the Flink DAG: Kafka source → `ProcessFunction`(s) → Kafka sinks. Has a `main` object as entry point.
3. **`*Function.scala`** — Extends `BaseProcessFunction`; implements `processElement` with business logic and `metricsList` with counter names.

### Configuration

Each job has a HOCON config file in `src/main/resources/<job-name>.conf` that extends `base-config.conf` (from `jobs-core`). Config is loaded via `ConfigFactory`; environment variables take precedence via `withFallback(ConfigFactory.systemEnvironment())`.

Key infra defaults (overridden per environment):
- Kafka: `localhost:9092`
- Cassandra: `localhost:9042`
- Redis: `localhost:6379`
- Elasticsearch: `localhost:9200`
- JanusGraph: `localhost:8182`

### Tech Stack

- **Language:** Scala 2.12 / Java 11
- **Stream Processing:** Apache Flink 1.15.2
- **Messaging:** Apache Kafka 3.x
- **Graph DB:** JanusGraph (CQL backend on Cassandra)
- **Cache:** Redis
- **Search:** Elasticsearch 6.x
- **Storage:** Cloud (AWS S3 / Azure Blob / GCS) via pluggable `CloudStorageUtil`

## CI/CD

- **PR workflow** (`pr-actions.yml`): Builds `jobs-core` first, then runs per-module test + coverage + SonarCloud analysis in parallel. Uses JDK 11 for tests, JDK 17 for Sonar.
- **Image build** (`build-push-img.yml`): Triggers on git tag push; builds all modules, assembles `jobs-distribution` tar.gz, builds Docker image, pushes to configured registry (DockerHub / GHCR / GCP / Azure).
- `$COVERAGE-OFF$` / `$COVERAGE-ON$` comments are used to exclude Flink cluster bootstrap code (`main` objects and `StreamTask` wiring) from scoverage.
