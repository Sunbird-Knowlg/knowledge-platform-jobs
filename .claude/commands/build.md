---
description: Build knowledge-platform-jobs (skip tests), optional module + cloud profile
argument-hint: "[module] [profile: aws|gcloud|azure|oci]"
allowed-tools: Bash(mvn *)
---

Build **knowledge-platform-jobs**. Args — module: `$1`, cloud profile: `$2` (either may be empty).

The cloud profile selects the storage connector (default is `azure`). Choose the command:
- no args → `mvn clean install -DskipTests`
- module only → `mvn clean install -DskipTests -pl $1 -am`
- module + profile → `mvn clean install -DskipTests -pl $1 -am -P$2`
- profile only → `mvn clean install -DskipTests -P$2`

Modules: `jobs-core`, `publish-pipeline`, `content-embedding-job`, `transaction-event-processor`, `qrcode-image-generator`, `dialcode-context-updater`, `cassandra-data-migration`, `asset-enrichment`, `video-stream-generator`, `jobs-distribution`. Job jars land at `<module>/target/<name>-1.0.0.jar`; `jobs-distribution` packages all jobs into one image.

Run the command. On success report it briefly. On failure, **name the failing module** and quote the first relevant error block — not the whole reactor log. (Note: Flink deps are `provided`; a build is fine, but running locally needs the temporary edits from the README.)

## Examples

```
/build                                    # full build, default (azure) profile
/build transaction-event-processor        # one module + its deps
/build transaction-event-processor aws    # one module, S3 connector profile
/build "" gcloud                          # full build, GCS connector profile
```
