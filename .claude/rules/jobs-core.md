---
paths:
  - "jobs-core/**"
---

# jobs-core — shared Flink base

`jobs-core` is the shared Flink base (`org.sunbird.job.*`): Kafka connectors, Redis cache, serde, cloud-storage, `base-config.conf`. Per-job configs layer over `jobs-core/.../base-config.conf`.

**Duplicated across repos.** This `jobs-core` is a *copy* of `data-pipeline/jobs-core` (not a shared published artifact) and the two have drifted. **Apply shared fixes to both.** See root `/CLAUDE.md`.

Cloud storage profiles (`aws`/`gcloud`/`azure` default/`oci`) live in `jobs-core/pom.xml` (and `jobs-distribution/pom.xml`) and select the connector dependency — pick one that matches the deploy target.

## Base-class pattern (adding a job / function)

All base classes are in `jobs-core/src/main/scala/org/sunbird/job/BaseProcessFunction.scala`.

- A new **function** extends `BaseProcessFunction[T, R](config)` (or
  `BaseProcessKeyedFunction`, `WindowBaseProcessFunction`, `TimeWindowBaseProcessFunction`)
  and implements `processElement(event, context, metrics: Metrics)` + `metricsList(): List[String]`.
- A new **job's config** extends `BaseJobConfig(config, "<job-name>")` (see `configuration-discipline.md`).
- A new **job entrypoint** is a `class <Job>StreamTask(config, kafkaConnector, …)` with a
  `process()` that calls `FlinkUtil.getExecutionContext(config)` + `env.execute(config.jobName)`,
  plus a companion `object` with `main(args)` (fenced `// $COVERAGE-OFF$`). Reference:
  `transaction-event-processor/.../task/TransactionEventProcessorStreamTask.scala:27,198`.
- Shared traits to reuse: `JobMetrics` (metric registration), `FailedEventHelper` (DLQ payload —
  see `error-handling.md`), and the serde schemas under `jobs-core/.../serde/`.

**Duplication caveat (critical):** these base classes are copied in `data-pipeline/jobs-core`.
A change to any base class here must be **mirrored** there — never edit in one repo only.
