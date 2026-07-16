---
paths:
  - "**/*.scala"
---

# Code documentation

Match existing practice — the code is **sparsely commented**, and that is fine.

- **Document the non-obvious (the *why*)**, not the mechanics. Good existing examples: the
  Kafka offset-reset semantics note in `jobs-core/.../connector/FlinkKafkaConnector.scala:18-25`,
  and the security-flag note in `TransactionEventProcessorConfig.scala:228-229`.
- Keep the `// section header` comments used to group blocks in `*Config` classes
  (`// Kafka Topics`, `// Metric List`).
- **Do not** mandate full Scaladoc on every function/config class — that would contradict the
  codebase style. Add a comment where behavior is surprising or a value is security/correctness
  sensitive.
