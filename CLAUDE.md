# CLAUDE.md — knowledge-platform-jobs

Flink stream jobs for the **content** lifecycle (Scala 2.13 / Maven / Flink 1.18.1). See root `/CLAUDE.md` for cross-repo context, the Kafka/Flink backbone, and the "jobs-core is duplicated" note.

## Maven modules (root `pom.xml`, profile `knowlg-jobs`)

| Module | Role |
|---|---|
| `jobs-core` | Shared Flink base (`org.sunbird.job.*`): Kafka connectors, Redis cache, serde, cloud-storage, `base-config.conf`. Duplicated in `data-pipeline` — mirror fixes. |
| `publish-pipeline/publish-core` | Shared publish base classes + cloud-storage helpers |
| `publish-pipeline/knowlg-publish` | Publishes content, collections, assets |
| `publish-pipeline/live-node-publisher` | Re-publishes live nodes on metadata update |
| `content-embedding-job` | Content embedding generation; submodules: `-api`, `embedding-services`, `quantization-strategies`, `chunking-strategies`, `content-embedding-core` |
| `asset-enrichment` | Enriches image/video assets (dimensions, duration, thumbnails) |
| `video-stream-generator` | Generates streaming URLs for uploaded mp4/webm |
| `transaction-event-processor` | Audit events + ES sync for every JanusGraph transaction (CDC consumer) |
| `qrcode-image-generator` | Generates QR images for dial codes |
| `dialcode-context-updater` | Updates dial-code context in the graph |
| `cassandra-data-migration` | One-off Cassandra/Yugabyte data migration job |
| `jobs-distribution` | Packages all jobs into one deployable Docker image (`docker/`, `kubernets/`) |

## Commands

Slash-commands live in `.claude/commands/` (type `/<name>`):

| Command | Use |
|---|---|
| `/commit [hint]` | Conventional Commits commit (confirms first; no AI trailer; warns on `jobs-core` mirror) |
| `/build [module] [profile]` | `mvn clean install -DskipTests` (profile: `aws\|gcloud\|azure\|oci`, default azure) |
| `/test [module] [TestSpec]` | Run tests, scoped; summarize failures |
| `/coverage` | `mvn ... scoverage:report` + per-module summary |
| `/pr [base-branch]` | Open a PR (base defaults to `develop`); enforces a JIRA key in title/branch |

## Rules

Component rules live in `.claude/rules/` (auto-discovered):

| Rule file | Loads | Covers |
|---|---|---|
| `.claude/rules/job-conventions.md` | Always | Idempotency/DLQ conventions + repo-wide layout (per-job config, StreamTask, Kafka topics) |
| `.claude/rules/flink-build.md` | On opening any `pom.xml` | Build/test commands, cloud storage profiles, jar output, local-run temporary edits |
| `.claude/rules/jobs-core.md` | On opening files under `jobs-core/**` | Shared Flink base + base-class pattern for new jobs/functions + "duplicated across repos, mirror fixes" caveat |
| `.claude/rules/configuration-discipline.md` | On `*Config.scala` / `src/main/resources/**` | One `*Config extends BaseJobConfig` per job; no ad-hoc config reads; HOCON layering |
| `.claude/rules/logging-observability.md` | On `**/*.scala` | slf4j lazy logger + `LoggerUtil` envelopes; string-counter metrics via `metricsList()`/`incCounter` |
| `.claude/rules/error-handling.md` | On `**/functions/**`, `**/function/**` | DLQ side-output pattern (`failedEventOutTag`); router-rethrow anti-pattern |
| `.claude/rules/testing-requirements.md` | On test files (`*Spec.scala`, `src/test/**`) | `MiniCluster` + mocked `FlinkKafkaConnector` + `MockWebServer` harness; Scoverage |
| `.claude/rules/code-documentation.md` | On `**/*.scala` | Sparse-by-design; comment the non-obvious, keep `*Config` section headers |
