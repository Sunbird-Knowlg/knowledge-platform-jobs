---
paths:
  - "**/functions/**"
  - "**/function/**"
---

# Error handling (functions)

Deepens the idempotency/DLQ note in `job-conventions.md`. In a Flink function, a bad event
must go to the **dead-letter side-output**, never crash the pipeline.

## Standard — DLQ side-output

In `processElement`, catch the failure, build a failed-event payload, emit it, and return:

```scala
catch {
  case ex: Throwable =>
    val failedEvent = getFailedEvent(config.jobName, event.getMap(), ex)   // FailedEventHelper trait
    context.output(config.failedEventOutTag, failedEvent)
    return
}
```

- `getFailedEvent(...)` comes from the `FailedEventHelper` trait
  (`jobs-core/src/main/scala/org/sunbird/job/helper/FailedEventHelper.scala`); it serializes
  the event with a `failInfo` block.
- `failedEventOutTag: OutputTag[String]` is declared in the job's `*Config`
  (e.g. `TransactionEventProcessorConfig.scala:88`).
- Domain exceptions live in `jobs-core/src/main/scala/org/sunbird/job/exception/`:
  `ExceptionCases.scala` (`APIException`, `CassandraException`, `ElasticSearchException`,
  `ServerException`), plus `InvalidEventException` / `InvalidInputException`.
- **Good examples**: `CompositeSearchIndexerFunction.scala:97-98`,
  `DIALCodeIndexerFunction.scala:59-60`, `LiveCollectionPublishFunction.scala:158`.

## Anti-pattern to flag (do NOT copy)

Routers that **rethrow** instead of side-outputting crash and restart the whole pipeline on
a single poison event — `transaction-event-processor/.../TransactionEventRouter.scala:54-62`
(`throw new InvalidEventException(...)`). Prefer the side-output pattern above; only rethrow
for genuinely unrecoverable, non-event-specific failures.
