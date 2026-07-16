---
paths:
  - "**/*.scala"
---

# Logging & observability

## Logging

- Per-class slf4j logger:
  `private[this] lazy val logger = LoggerFactory.getLogger(classOf[X])`
  (e.g. `TransactionEventRouter.scala:20-21`).
- Structured telemetry log envelopes are built via
  `jobs-core/src/main/scala/org/sunbird/job/util/LoggerUtil.scala`
  (`getErrorLogs`/`getEntryLogs`/`getExitLogs` with `eid`/`edata`).

## Metrics

Business metrics are **string counters**, not ad-hoc Flink metrics:

1. Declare the counter name as a constant in the job's `*Config` (see `configuration-discipline.md`),
   e.g. `TransactionEventProcessorConfig.scala:93-133`.
2. Return it from the function's `metricsList(): List[String]`.
3. Increment with `metrics.incCounter(config.<name>Count)` inside `processElement`.

`BaseProcessFunction.open()` registers each as a Flink `Gauge[Long]` under a metric group
named `config.jobName` (`jobs-core/.../BaseProcessFunction.scala:37-44`) — so a new counter
only needs the three steps above.
