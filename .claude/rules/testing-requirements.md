---
paths:
  - "**/*Spec.scala"
  - "**/src/test/**"
---

# Testing requirements

Stack: **ScalaTest `FlatSpec` + `Matchers` + `MockitoSugar`**. Extend the shared
`jobs-core/src/test/scala/org/sunbird/spec/BaseTestSpec.scala`.

## Standard

- **Cover normal, edge, and failure paths** — including the DLQ side-output branch
  (see `error-handling.md`).
- **Job-level tests run on a real Flink mini-cluster**: `MiniClusterWithClientResource`
  (from `flink-test-utils`), e.g.
  `transaction-event-processor/src/test/scala/org/sunbird/job/spec/TransactionEventProcessorTaskTestSpec.scala:35`.
- **Mock the boundaries, don't hit real infra**: `mock[FlinkKafkaConnector]` feeding a
  `SourceFunction` for input; `okhttp3.mockwebserver.MockWebServer` for HTTP. The StreamTask
  exposes a `processForTest(env, inputStream)` entrypoint for tests. **No EmbeddedKafka** —
  Kafka is mocked.
- Assert metric counters via `jobs-core/.../spec/BaseMetricsReporter.scala`; put sample
  events in a per-job `EventFixture`.
- Tests live under `<module>/src/test/scala/org/sunbird/job/spec/`.
- Coverage is **Scoverage** (SonarQube reads `**/target/scoverage.xml`). Fence `main()`
  objects with `// $COVERAGE-OFF$ … // $COVERAGE-ON$`.
