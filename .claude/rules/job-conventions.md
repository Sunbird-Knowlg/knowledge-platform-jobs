# Job conventions & layout (repo-wide)

## Idempotency & DLQ
Jobs are generally **not idempotent** and lack DLQ discipline — replays can duplicate work. Prefer side-output DLQ (`context.output(failedEventOutTag, …)` + `return`) over rethrow, which crashes the pipeline on a poison event. See root `/CLAUDE.md`.

## Layout that matters
- Per-job config: `<module>/src/main/resources/<job-name>.conf` (layers over `jobs-core/.../base-config.conf`); test config in `src/test/resources/test.conf`.
- StreamTask entrypoint per job: `<module>/src/main/scala/.../*StreamTask.scala`.
- Kafka topics are `<env>.<name>` (e.g. `sunbirddev.publish.job.request`).
