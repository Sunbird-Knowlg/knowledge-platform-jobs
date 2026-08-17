---
description: Create a Conventional Commits git commit for the current changes
argument-hint: "[optional message hint]"
allowed-tools: Bash(git status:*), Bash(git diff:*), Bash(git log:*), Bash(git add:*), Bash(git commit:*)
---

You are creating a git commit for **knowledge-platform-jobs** (Scala/Flink/Maven). Extra hint from the user (may be empty): $ARGUMENTS

## Pre-loaded context

Status:
!`git status --porcelain=v1 -b`

Staged diff:
!`git diff --staged`

Recent commit style (mirror it):
!`git log --oneline -12`

## Steps

1. Analyze the changes above and determine the commit **type** and **scope**.
2. If **nothing is staged**, propose the files to stage and **ask the user before running `git add`**. Never stage secrets (`.env*`, `*.pem`, `*.key`), and never stage the temporary **local-run pom edits** (added `flink-clients`, commented-out `provided` scope, `createLocalEnvironment()`).
3. By default **don't stage `.md` files** — ask the user before including any documentation changes.
4. If the change touches `jobs-core`, remind the user it is **duplicated in `data-pipeline`** and likely needs mirroring there.
5. Present the proposed commit message and **wait for confirmation before committing**.
6. Create the commit with `git commit`.

## Commit Message Format

```
{type}({scope}): {short description}

{optional body — only if the change needs explanation}
```

### Types
- `feat` — new feature or job capability
- `fix` — bug fix
- `refactor` — code restructure without behavior change
- `style` — formatting only, no logic change
- `test` — adding or fixing tests
- `chore` — build, config, dependency changes
- `docs` — documentation only

### Scope
Use the job/module being changed:
- `jobs-core` — shared Flink base (BaseProcessFunction, BaseJobConfig, serde) — **duplicated in data-pipeline**
- `publish` — publish-pipeline (knowlg-publish, live-node-publisher, publish-core)
- `content-embedding` — content-embedding job
- `transaction-event` — transaction-event-processor (CDC → Elasticsearch)
- `qrcode` — qrcode-image-generator
- `dialcode` — dialcode-context-updater
- `asset-enrichment` — asset-enrichment job
- `video-stream` — video-stream-generator
- `cassandra-migration` — cassandra-data-migration
- `build` — Maven / pom / cloud-profile / dependency changes
- `ci` — GitHub Actions / CircleCI / SonarCloud workflows

Omit the scope only if the change is genuinely cross-cutting.

### Rules
- Subject line: max 72 characters, imperative mood ("add", not "added" or "adds")
- No period at the end of the subject line
- Body (if needed): explain *why*, not *what* — the diff shows what
- Reference issue numbers if relevant: `fixes #123`
- Do **NOT** add a copyright header to any file
- Do **NOT** add `Co-Authored-By: Claude` or any Claude/AI authorship trailer

## Examples

```
fix(transaction-event): validate objectType before routing enrich-only events
```

```
fix(content-embedding): retry OpenAI 429/5xx with exponential backoff and jitter

Transient upstream errors were failing the whole event; back off and
retry so a single 429 doesn't route the content to the DLQ.
```

```
refactor(jobs-core): remove blind shutdown-hook sleep

Mirror this change into data-pipeline/jobs-core — the base is duplicated.
```

```
chore(build): bump Netty to 4.1.131.Final
```

```
test(qrcode): add spec for the QR image-generation DLQ side-output path
```

---

After analyzing the changes, present the proposed commit message to the user for confirmation before committing.
