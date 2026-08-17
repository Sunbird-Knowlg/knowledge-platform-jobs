---
description: Generate the Scoverage coverage report and summarize it
allowed-tools: Bash(mvn *)
---

Generate test coverage for **knowledge-platform-jobs**:

```
mvn clean install scoverage:report
```

Per-module reports land under each module's `target/` (`scoverage.xml`).

After it finishes, report the **per-module line-coverage %** and call out any module notably low. Remember `main()` objects are intentionally fenced with `// $COVERAGE-OFF$ … // $COVERAGE-ON$`, so entrypoints won't count. **SonarCloud** (`sonar-project.properties` / `.github/workflows/sonarcloud.yml`) is the authoritative gate in CI — this local report is for a quick check.

## Examples

```
/coverage    # build with coverage, then summarize per-module line %
```
