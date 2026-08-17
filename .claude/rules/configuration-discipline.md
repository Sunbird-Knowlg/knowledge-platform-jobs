---
paths:
  - "**/*Config.scala"
  - "**/src/main/resources/**"
---

# Configuration discipline

Each job centralizes **all** its configuration in one `*Config` class — never read config
ad hoc inside functions.

## Standard

- One `class <Job>Config(override val config: Config) extends BaseJobConfig(config, "<job-name>")`
  per job (`jobs-core/src/main/scala/org/sunbird/job/BaseJobConfig.scala`). It reads every
  key with a guarded default (`if (config.hasPath(k)) config.getX(k) else default`, or the
  base `getString/getInt/getBoolean(key, default)` helpers) and declares, in that one class:
  - all Kafka topic + ES index names,
  - all `OutputTag`s (including `failedEventOutTag` — see `error-handling.md`),
  - all metric-name string constants (see `logging-observability.md`).
  Reference: `transaction-event-processor/.../TransactionEventProcessorConfig.scala:12,88,93-133`.
- **Functions receive the `*Config`; they do not read raw config.** No `ConfigFactory`
  calls in functions.
- **HOCON layering:** base defaults in `jobs-core/src/main/resources/base-config.conf`; each
  job overlays `<module>/src/main/resources/<job-name>.conf`; test overrides in
  `src/test/resources/test.conf`.
- **Load config only in `main()`**: `ConfigFactory.load("<job>.conf").withFallback(systemEnvironment())`,
  or from `--config.file.path` (`TransactionEventProcessorStreamTask.scala:200-215`).
