---
description: Run tests, optionally scoped to a module and/or test class; summarize failures
argument-hint: "[module] [TestSpec]"
allowed-tools: Bash(mvn *)
---

Run tests for **knowledge-platform-jobs**. Args — module: `$1`, test class: `$2` (either may be empty).

Choose the command:
- no args → `mvn test`
- module only → `mvn test -pl $1`
- module + class → `mvn test -pl $1 -Dtest=$2`
  (single method: `mvn test -pl $1 -Dtest='$2#*pattern*'`)

Stack is ScalaTest `FlatSpec` + Mockito; job specs run on a Flink `MiniClusterWithClientResource` with a mocked `FlinkKafkaConnector` and `MockWebServer` (see `.claude/rules/testing-requirements.md`). Kafka is mocked — no embedded broker.

After running, summarize total / passed / failed. For any failure, surface the **spec name** and the first assertion/exception line.

## Examples

```
/test                                                       # all tests
/test transaction-event-processor                           # one module
/test transaction-event-processor TransactionEventProcessorTaskTestSpec   # one spec
```
