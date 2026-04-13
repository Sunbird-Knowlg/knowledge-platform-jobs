# Knowledge Platform Jobs

Apache Flink stream processing jobs for the Sunbird Knowledge Platform. Each job consumes events from Kafka, processes content lifecycle operations, and writes results to Yugabyte, JanusGraph, Elasticsearch.

---

## Table of Contents

1. [Modules](#modules)
2. [Prerequisites](#prerequisites)
3. [Local Development Setup](#local-development-setup)
   - [Step 1 — Clone the repository](#step-1--clone-the-repository)
   - [Step 2 — Start infrastructure](#step-2--start-infrastructure)
   - [Step 3 — Initialize YugabyteDB keyspaces](#step-3--initialize-yugabytedb-keyspaces)
   - [Step 4 — Initialize Elasticsearch indices](#step-4--initialize-elasticsearch-indices)
   - [Step 5 — Create Kafka topics](#step-5--create-kafka-topics)
   - [Step 6 — Build the project](#step-6--build-the-project)
   - [Step 7 — Run a job](#step-7--run-a-job)
4. [Redis (optional)](#redis-optional)
5. [Cloud Storage Configuration](#cloud-storage-configuration)
6. [Building the Docker Image](#building-the-docker-image)
7. [CI/CD — GitHub Actions](#cicd--github-actions)

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
- **Docker Desktop** — verify with `docker --version`
  - Allocate at least **6 GB RAM** to Docker Desktop (Settings > Resources > Memory). The default 3.8 GB is not enough.
- **Git** — verify with `git --version`

---

## Local Development Setup

Follow these steps in order. The full setup takes about 5 minutes.

### Step 1 — Clone the repository

```shell
git clone https://github.com/Sunbird-Knowlg/knowledge-platform-jobs.git
cd knowledge-platform-jobs
```

### Step 2 — Start infrastructure

```shell
cd docker
docker compose up -d
```

This starts **Elasticsearch**, **YugabyteDB**, **JanusGraph**, and **Kafka**.

Wait about **90 seconds** for everything to initialize (YugabyteDB starts first, then JanusGraph connects to it and creates the graph schema). You can check progress with:

```shell
docker compose ps                  # all containers should show "Up"
docker logs janusgraph | grep "SCHEMA INITIALIZATION"
# Expected: --- SCHEMA INITIALIZATION COMPLETE ---
```

### Step 3 — Initialize YugabyteDB keyspaces

Still inside the `docker/` directory, run the migration script to create the required keyspaces and tables:

```shell
./init-yugabyte.sh
```

This downloads CQL migration files from [sunbird-spark-installer](https://github.com/Sunbird-Spark/sunbird-spark-installer/tree/develop/scripts/sunbird-yugabyte-migrations/sunbird-knowlg) and executes them. By default it uses `dev` as the keyspace prefix (e.g. `dev_content_store`) and the `develop` branch.

```shell
./init-yugabyte.sh sb           # use 'sb' as keyspace prefix instead
./init-yugabyte.sh dev main     # use a different branch
```

You only need to run this once. Run it again after `docker compose down -v` (which deletes volumes).

### Step 4 — Initialize Elasticsearch indices

Still inside the `docker/` directory, run the Elasticsearch init script to create the required indices and mappings:

```shell
./init-elasticsearch.sh
```

This downloads index and mapping definitions from [sunbird-devops](https://github.com/project-sunbird/sunbird-devops/tree/release-8.0.0/ansible/roles/es7-mapping/files) and applies them via the Elasticsearch REST API. By default it uses the `release-8.0.0` branch.

```shell
./init-elasticsearch.sh release-9.0.0    # use a different branch
```

You only need to run this once. Run it again after `docker compose down -v` (which deletes volumes).

### Step 5 — Create Kafka topics

```shell
docker exec -it kafka sh
# Inside the container:
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sunbirddev.publish.job.request
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sunbirddev.knowlg.republish.request
# Add more topics as needed for the job you are running
exit
```

### Step 6 — Build the project

Go back to the repository root and build:

```shell
cd ..
mvn clean install -DskipTests
```

This takes a few minutes the first time (Maven downloads dependencies). A successful build ends with `BUILD SUCCESS`. All job jars will be in their respective `target/` directories.

To build for a specific cloud provider:
```shell
mvn clean install -DskipTests -Paws      # AWS S3
mvn clean install -DskipTests -Pgcloud   # Google Cloud Storage
```
If no profile is specified, the default build targets Azure.

### Step 7 — Run a job

#### Option A — Standalone Flink cluster

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

4. Submit the job. Example for `knowlg-publish`:
   ```shell
   ./bin/flink run -m localhost:8081 \
     ../publish-pipeline/knowlg-publish/target/knowlg-publish-1.0.0.jar
   ```
   Verify: the job should appear in the Flink dashboard at http://localhost:8081 with status `RUNNING`.

5. Produce a test event:
   ```shell
   docker exec -it kafka sh
   # Inside the container:
   kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sunbirddev.publish.job.request
   # Type a JSON event and press Enter
   ```

   Watch the Flink task logs in the dashboard (`Job > Task Managers > Logs`).

#### Option B — IntelliJ (recommended for debugging)

> Use this option when you want to step through code with a debugger.

1. Open the project in IntelliJ (`File > Open` > select the root `pom.xml`).

2. In the job's `pom.xml` (e.g. `publish-pipeline/knowlg-publish/pom.xml`), make these **temporary** changes:

   > Do not commit these changes. Revert them before raising a PR.

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

   > Do not commit this change either.

   ```scala
   // implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
   implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
   ```

4. Set the [cloud storage environment variables](#cloud-storage-configuration) in IntelliJ's run configuration (`Run > Edit Configurations > Environment variables`).

5. Right-click the StreamTask file > `Run` or `Debug`.

6. Produce a test event to trigger the job:
   ```shell
   docker exec -it kafka sh
   # Inside the container:
   kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sunbirddev.publish.job.request
   # Type a JSON event and press Enter
   ```

   Watch the IntelliJ console for output.

---

### Service URLs

| Service | URL |
|---------|-----|
| Elasticsearch | http://localhost:9200 |
| YugabyteDB UI | http://localhost:9001 |
| JanusGraph (Gremlin) | ws://localhost:8182/gremlin |
| Kafka | localhost:9092 |

### Stopping and resetting

```shell
cd docker
docker compose down            # stop containers, keep data
docker compose down -v         # stop containers and delete all data
```

---

## Redis (optional)

Redis is disabled by default (`redis.enabled = false` in `jobs-core/src/main/resources/base-config.conf`). Only start it if the job you are running explicitly enables it.

```shell
cd docker
docker compose --profile redis up -d
```

---

## Cloud Storage Configuration

Cloud storage is needed for jobs that upload/download content artifacts. If you are only testing event processing that doesn't involve file uploads, you can skip this.

Set these environment variables before running a job:

#### Azure (default)
```shell
export cloud_storage_type=azure
export cloud_storage_auth_type=ACCESS_KEY
export azure_storage_key=your-account-name
export azure_storage_secret=your-account-key
export azure_storage_container=your-container-name
```

#### AWS S3
```shell
export cloud_storage_type=aws
export cloud_storage_auth_type=ACCESS_KEY
export aws_storage_key=your-access-key-id
export aws_storage_secret=your-secret-access-key
export aws_storage_container=your-s3-bucket-name
```

#### Google Cloud Storage
```shell
export cloud_storage_type=gcloud
export cloud_storage_auth_type=ACCESS_KEY
export gcloud_storage_key=your-client-email
export gcloud_storage_secret=/path/to/key.json
export gcloud_storage_container=your-gcs-bucket-name
```

#### Additional
```shell
export content_youtube_apikey=your-api-key    # required for video-stream-generator and asset-enrichment
```

---

## Building the Docker Image

The `jobs-distribution` module packages all jobs into a single Docker image. The build is split by cloud provider — only the jars and plugins required for that cloud are included.

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

### Required variables (Settings > Secrets and variables > Actions)

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
