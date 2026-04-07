# Knowledge Platform Jobs

Apache Flink stream processing jobs for the Sunbird Knowledge Platform. Each job consumes events from Kafka, processes content lifecycle operations, and writes results to Yugabyte, JanusGraph, Elasticsearch, or cloud storage.

---

## Table of Contents

1. [Modules](#modules)
2. [Prerequisites](#prerequisites)
3. [Local Development Setup](#local-development-setup)
   - [Start all services](#start-all-services)
   - [Redis (optional)](#redis-optional)
   - [Create Kafka topics](#create-kafka-topics)
4. [Building the Project](#building-the-project)
5. [Running a Job Locally](#running-a-job-locally)
   - [Option A — Standalone Flink cluster](#option-a--standalone-flink-cluster)
   - [Option B — IntelliJ (recommended for debugging)](#option-b--intellij-recommended-for-debugging)
6. [Cloud Storage Configuration](#cloud-storage-configuration)
7. [Building the Docker Image](#building-the-docker-image)
8. [CI/CD — GitHub Actions](#cicd--github-actions)

---

## Modules

| Module | Description |
|--------|-------------|
| `jobs-core` | Shared Flink utilities, Kafka connectors, Redis cache, serde, base config |
| `publish-pipeline/publish-core` | Shared publishing logic (base classes, cloud storage helpers) |
| `publish-pipeline/knowlg-publish` | Publishes content, collections, and assets |
| `publish-pipeline/live-node-publisher` | Re-publishes live nodes on metadata update |
| `asset-enrichment` | Enriches image and video assets (dimensions, duration, thumbnails) |
| `video-stream-generator` | Generates streaming URLs for uploaded mp4/webm files |
| `transaction-event-processor` | Generates audit events and syncs data in elasticsearch for every JanusGraph transaction |
| `qrcode-image-generator` | Generates QR code images for dial codes |
| `dialcode-context-updater` | Updates dial code context in the graph |
| `jobs-distribution` | Packages all jobs into a single deployable Docker image |

---

## Prerequisites

Make sure these are installed before you begin:

- **Java 11** — verify with `java -version`
- **Maven 3.8+** — verify with `mvn -version`
- **Docker** — verify with `docker --version`

---

## Local Development Setup

All services are defined in `docker/docker-compose.yml`. Before starting, set the JanusGraph image:

```shell
# Open docker/docker-compose.yml and replace <janusgraph-image> with the actual image name
```

### Start all services

```shell
docker-compose -f docker/docker-compose.yml up -d
```

This starts Elasticsearch, Yugabyte, JanusGraph, and Kafka. JanusGraph automatically initializes the schema on startup via `docker/janusgraph/scripts/schema_init.groovy`.

Verify JanusGraph schema was initialized:
```shell
docker logs janusgraph | grep "SCHEMA INITIALIZATION"
# Expected: --- SCHEMA INITIALIZATION COMPLETE ---
```

### Redis (optional)

Redis is disabled by default (`redis.enabled = false` in `jobs-core/src/main/resources/base-config.conf`). Only start it if the job you are running explicitly enables it.

```shell
docker-compose -f docker/docker-compose.yml --profile redis up -d
```

### Create Kafka topics

```shell
docker exec -it kafka sh
# Inside the container:
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sunbirddev.publish.job.request
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sunbirddev.knowlg.republish.request
# Add more topics as needed for the job you are running
exit
```

### Service URLs

| Service | URL |
|---------|-----|
| Elasticsearch | http://localhost:9200 |
| YugabyteDB UI | http://localhost:9000 |
| JanusGraph (Gremlin) | ws://localhost:8182/gremlin |
| Kafka | localhost:9092 |

---

## Building the Project

From the repository root:

```shell
mvn clean install -DskipTests
```

A successful build ends with `BUILD SUCCESS`. All job jars will be in their respective `target/` directories.

---

## Running a Job Locally

### Option A — Standalone Flink cluster

> Use this option to run the job the same way it runs in production.

1. Download and extract Flink 1.18.1:
   ```shell
   wget https://dlcdn.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
   tar xzf flink-1.18.1-bin-scala_2.12.tgz
   ```

2. Start the Flink cluster:
   ```shell
   cd flink-1.18.1
   ./bin/start-cluster.sh
   ```
   Verify: open http://localhost:8081 — you should see the Flink dashboard with 1 TaskManager.

3. Set the [cloud storage environment variables](#cloud-storage-configuration).

4. Make sure all containers from [Local Development Setup](#local-development-setup) are running.

5. Submit the job. Example for `knowlg-publish`:
   ```shell
   ./bin/flink run -m localhost:8081 \
     ../publish-pipeline/knowlg-publish/target/knowlg-publish-1.0.0.jar
   ```
   Verify: the job should appear in the Flink dashboard at http://localhost:8081 with status `RUNNING`.

6. Produce a test event:
   ```shell
   docker exec -it kafka sh
   # Inside the container:
   kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sunbirddev.publish.job.request
   # Type a JSON event and press Enter
   ```

   Watch the Flink task logs in the dashboard (`Job → Task Managers → Logs`).

---

### Option B — IntelliJ (recommended for debugging)

> Use this option when you want to step through code with a debugger.

1. Open the project in IntelliJ (`File → Open` → select the root `pom.xml`).

2. In the job's `pom.xml` (e.g. `publish-pipeline/knowlg-publish/pom.xml`), make these **temporary** changes:

   > ⚠️ Do not commit these changes. Revert them before raising a PR.

   Add `flink-clients` as a dependency:
   ```xml
   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-clients_${scala.version}</artifactId>
     <version>${flink.version}</version>
   </dependency>
   ```

   Comment out the `provided` scope on `flink-streaming-scala`:
   ```xml
   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-streaming-scala_${scala.version}</artifactId>
     <version>${flink.version}</version>
     <!-- <scope>provided</scope> -->
   </dependency>
   ```

3. In the job's StreamTask file (e.g. `KnowlgPublishStreamTask.scala`), switch to a local execution environment:

   > ⚠️ Do not commit this change either.

   ```scala
   // implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
   implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
   ```

4. Set the [cloud storage environment variables](#cloud-storage-configuration) in IntelliJ's run configuration (`Run → Edit Configurations → Environment variables`).

5. Make sure all containers from [Local Development Setup](#local-development-setup) are running.

6. Right-click the StreamTask file → `Run` or `Debug`.

7. Produce a test event to trigger the job:
   ```shell
   docker exec -it kafka sh
   # Inside the container:
   kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sunbirddev.publish.job.request
   # Type a JSON event and press Enter
   ```

   Watch the IntelliJ console for output.

---

## Cloud Storage Configuration

Set these environment variables before running any job locally. In Kubernetes, authentication is handled automatically via Workload Identity and these are not needed.

```shell
export cloud_storage_type=        # azure | gcloud | aws
export cloud_storage_auth_type=ACCESS_KEY

# Azure
export azure_storage_key=
export azure_storage_secret=
export azure_storage_container=

# AWS
export aws_storage_key=
export aws_storage_secret=
export aws_storage_container=

# GCP
export gcloud_storage_key=
export gcloud_storage_secret=
export gcloud_storage_container=

export content_youtube_apikey=    # required for video-stream-generator and asset-enrichment
```

---

## Building the Docker Image

The `jobs-distribution` module packages all jobs into a single Docker image. The build is split by cloud provider — only the jars and plugins required for that cloud are included.

The Maven command does everything in one shot: `-am` builds all upstream job modules first, then `install` compiles and packages `jobs-distribution` (producing the tar.gz and staging any cloud-specific jars). The Docker `--target` must match the Maven profile used.

**Azure (default)**
```shell
mvn clean install -DskipTests -pl jobs-distribution -am
docker build --target azure -t knowledge-platform-jobs:azure jobs-distribution/
```

**GCP**
```shell
mvn clean install -DskipTests -pl jobs-distribution -am -Pgcloud
docker build --target gcloud -t knowledge-platform-jobs:gcloud jobs-distribution/
```

**AWS**
```shell
mvn clean install -DskipTests -pl jobs-distribution -am -Paws
docker build --target aws -t knowledge-platform-jobs:aws jobs-distribution/
```

---

## CI/CD — GitHub Actions

The `build-push-img` workflow runs on every Git tag push. It builds all modules, packages the distribution, and pushes the Docker image to a container registry.

### Required variables (Settings → Secrets and variables → Actions)

| Variable | Description |
|----------|-------------|
| `CSP` | Cloud provider: `azure` (default), `gcloud`, or `aws` |
| `REGISTRY_PROVIDER` | Registry type: `azure`, `gcp`, `dockerhub`, or leave unset for GHCR |

### Registry credentials

**GitHub Container Registry (GHCR)** — default, no setup needed. Uses the built-in `GITHUB_TOKEN`.

**DockerHub**

| Secret | Example |
|--------|---------|
| `REGISTRY_USERNAME` | `myusername` |
| `REGISTRY_PASSWORD` | DockerHub password or access token |
| `REGISTRY_NAME` | `docker.io` |
| `REGISTRY_URL` | `docker.io/myusername` |

**Azure Container Registry**

| Secret | Example |
|--------|---------|
| `REGISTRY_USERNAME` | ACR username |
| `REGISTRY_PASSWORD` | ACR password |
| `REGISTRY_NAME` | `myregistry.azurecr.io` |
| `REGISTRY_URL` | `myregistry.azurecr.io` |

**GCP Artifact Registry**

| Secret | Example |
|--------|---------|
| `GCP_SERVICE_ACCOUNT_KEY` | Base64-encoded service account JSON key |
| `REGISTRY_NAME` | `asia-south1-docker.pkg.dev` |
| `REGISTRY_URL` | `asia-south1-docker.pkg.dev/<project>/<repo>` |
