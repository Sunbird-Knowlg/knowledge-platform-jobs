# content-embedding-job

Apache Flink streaming job that generates semantic embedding vectors for Sunbird content objects
and writes them to OpenSearch, enabling hybrid keyword + vector (kNN) search on the
`compositesearch` index.

---

## Overview

The job consumes enriched content metadata events from Kafka, splits each object into text chunks,
calls an embedding API (OpenAI / Azure OpenAI / HuggingFace E5), compresses the resulting float32
vectors to int8, and stores them as a nested `chunks` field on the existing OpenSearch document.

```
Kafka (enriched.content.metadata)
  │
  ▼
ExtractFunction              — deserialize JSON → EnrichedMetadataEvent
  │ enrichedOutTag
  ▼
ChunkingFunction             — split metadata into TextChunks
  │ chunkedOutTag
  ▼  keyBy(objectId % parallelism)
BatchEmbeddingFunction       — buffer events, flush as single embedBatch API call
  │ embeddedOutTag             (up to batch_events per call, or window_size_ms timeout)
  ▼
QuantizationFunction         — float32 → int8 (4× compression)
  │ quantizedOutTag
  ▼  keyBy(constant 0)
BatchedOpenSearchSinkFunction — buffer docs, flush via BulkRequest
  │                             (up to bulk.size per request, or bulk.flush_interval_ms timeout)
  ├─ successOutTag → Kafka output topic (object IDs)
  └─ errorOutTag   → Kafka error topic (DLQ, all stages)
```

All inter-stage data travels via Flink **side outputs** (named `OutputTag`s).
The main stream is unused.

---

## Module Structure

| Module | Purpose |
|---|---|
| `content-embedding-job-api` | Domain case classes and service trait contracts |
| `embedding-services` | OpenAI, Azure OpenAI, and E5 embedding service implementations |
| `chunking-strategies` | Semantic and sliding-window chunking implementations |
| `quantization-strategies` | Int8 quantization implementation |
| `content-embedding-core` | Flink functions, job config, stream task (shaded fat jar) |

---

## Configuration

Config file: `content-embedding-core/src/main/resources/content-embedding.conf`

### Kafka

```hocon
kafka {
  input.topic  = "sunbirddev.enriched.content.metadata"
  output.topic = "sunbirddev.embedding.chunks"
  error.topic  = "sunbirddev.embedding.error"
  groupId      = "content-embedding-group"
}
```

### Chunking

```hocon
chunking {
  strategy = "semantic"          # or "sliding-window"

  semantic {
    max_chunk_size = 1000        # max characters per chunk (truncation)
  }

  sliding-window {
    max_tokens     = 512         # words per window (E5/OpenAI token limit)
    overlap_tokens = 102         # overlap between windows (~20%)
  }
}
```

**`semantic`** — field-based, one chunk per content section. Best for short metadata.

**`sliding-window`** — concatenates all text fields, slides a 512-word window with 20% overlap.
Best for long documents. Overlap preserves sentence context at boundaries.

### Embedding Service

```hocon
embedding {
  service    = "openai"     # or "e5"
  batch_size = 32           # max texts per embedBatch call (within one flush)

  # Batching — how many events to accumulate before a single API call
  batch_events   = 10       # flush after N events regardless of window
  window_size_ms = 5000     # flush after N ms even if batch is not full

  openai {
    api_key          = ${?OPENAI_API_KEY}
    model            = "text-embedding-3-small"
    dimensions       = 1536
    timeout          = 30
    # Azure OpenAI — leave blank for standard OpenAI
    azure_endpoint   = ${?AZURE_OPENAI_ENDPOINT}
    azure_deployment = ${?AZURE_OPENAI_DEPLOYMENT}
    azure_api_version = "2024-12-01-preview"
  }

  e5 {
    host       = "e5-embedding.semantic.svc.cluster.local"
    port       = 80
    dimensions = 768
    timeout    = 30
  }
}
```

**Azure OpenAI** is auto-detected when `azure_endpoint` is non-empty.
The endpoint URL becomes: `<azure_endpoint>/openai/deployments/<deployment>/embeddings?api-version=<version>`
and the auth header switches to `api-key: <key>`.

### Quantization

```hocon
quantization {
  strategy = "int8"
}
```

Int8 quantization automatically detects L2-normalised vectors (all OpenAI / E5 outputs)
and uses global scale 127: `byte = round(v × 127)`, achieving 4× compression with <2% recall loss.

### OpenSearch

```hocon
opensearch {
  host        = "localhost"
  port        = 9200
  index.name  = "compositesearch"
  user        = ""
  password    = ""

  # Bulk write batching
  bulk {
    size              = 50     # flush after N docs
    flush_interval_ms = 5000   # flush after N ms even if buffer not full
  }
}
```

---

## OpenSearch Index Requirements

The `compositesearch` index must have `index.knn: true` and a `chunks` nested mapping:

```json
"chunks": {
  "type": "nested",
  "properties": {
    "text":        { "type": "text" },
    "embedding":   { "type": "knn_vector", "dimension": 1536, "data_type": "byte",
                     "method": { "name": "hnsw", "space_type": "cosinesimil", "engine": "lucene" } },
    "token_count": { "type": "integer" },
    "chunk_index": { "type": "integer" }
  }
}
```

> Update `dimension` to match your embedding model (1536 for text-embedding-3-small, 768 for E5).

`index.knn: true` is a final setting — requires creating a new index and reindexing if the existing
index doesn't have it. See `OPENSEARCH_INDEX_SCHEMA_CHANGES.md` for the migration plan.

---

## Chunking Strategies

### Semantic (default)

Produces one chunk per logical content section:

| Content Type | Chunks Produced |
|---|---|
| Content | 1 — name + description + keywords + subject |
| Question | 1 — name + description + body + subject |
| Collection | 1 metadata + 1 per hierarchy child (recursive) |
| QuestionSet | 1 metadata + 1 per hierarchy child (recursive) |

### Sliding-Window

Concatenates all relevant fields into one text blob, then slides a window:

```
Text:    [word1 word2 ... word512 | word411 word412 ... word922 | ...]
          └─── chunk 0 ──────────┘ └────── chunk 1 ────────────┘
                                    ↑ 102-token overlap
```

Default: 512-token window, 102-token overlap (≈20%). Both values are configurable.

> **Note:** Whitespace-separated words are used as a token proxy — no external tokenizer dependency.
> This approximation is intentional: true BPE token counts vary per model but word counts
> are close enough to stay within the 512-token hard limit of E5 and OpenAI models.

---

## Batching Behaviour

### Embedding (`BatchEmbeddingFunction`)

Events are keyed by `Math.abs(objectId.hashCode) % embedding.parallelism` so each Flink task slot manages its own independent buffer. Two flush triggers:

1. **Size trigger:** buffer reaches `embedding.batch_events` → immediate flush, stale timer cancelled.
2. **Time trigger:** `embedding.window_size_ms` elapses since first event entered the buffer → flush whatever is in the buffer.

Without the time trigger, events that don't fill a complete batch (e.g. the last N events in a burst) would sit in Flink state indefinitely — never flushed, never embedded. `window_size_ms` guarantees that any event is processed within that many milliseconds even if more events never arrive.

**Observed throughput (450-event production run):** 46 API calls in ~53s vs ~450 calls in ~7.5 min without batching (~10× improvement).

### OpenSearch (`BatchedOpenSearchSinkFunction`)

All docs funnel into a single key (constant `0`, `sink.parallelism=1`). Same two-trigger pattern:

1. Buffer reaches `opensearch.bulk.size` → `BulkRequest` sent immediately.
2. `opensearch.bulk.flush_interval_ms` elapses → partial buffer flushed.

Each `BulkRequest` uses `WAIT_UNTIL` refresh policy set **on the `BulkRequest` itself** (not on individual items — OpenSearch rejects per-item refresh policy inside a bulk request).

Partial failures: `bulkUpdateWithRefresh` returns a `Map[objectId → Exception]` for failed items. Each failed item is routed individually to the DLQ; successful items in the same batch are not discarded.

---

## Adding a New Embedding Provider

1. Add a new `case class` in `EmbeddingServiceConfig` if new config fields are needed.
2. Implement `EmbeddingService` in the `embedding-services` module.
3. Add a `case` to `EmbeddingServiceFactory.getService`.
4. Add config block to `content-embedding.conf` and `test.conf`.
5. Wire config fields in `ContentEmbeddingConfig.embeddingServiceConfig`.

---

## Building

```bash
mvn clean install -f content-embedding-job/pom.xml
```

The shaded fat jar is at:
```
content-embedding-job/content-embedding-core/target/content-embedding-core-1.0.jar
```

It is also included in `jobs-distribution-1.0.tar.gz` which is extracted into
`$FLINK_HOME/lib/` in the Docker image during CI.

---

## Deployment

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENAI_API_KEY` | Yes (OpenAI mode) | OpenAI or Azure OpenAI API key |
| `AZURE_OPENAI_ENDPOINT` | Azure only | e.g. `https://<resource>.openai.azure.com/` |
| `AZURE_OPENAI_DEPLOYMENT` | Azure only | Deployment name, e.g. `text-embedding-3-small` |
| `E5_EMBEDDING_HOST` | E5 mode | TEI server hostname |
| `E5_EMBEDDING_PORT` | E5 mode | TEI server port |

### Flink Job Entry Point

```
org.sunbird.job.contentembedding.task.ContentEmbeddingStreamTask
```

Pass `--config.file.path /path/to/content-embedding.conf` or rely on classpath defaults.

---

## Parallelism

All stages have independent parallelism settings:

```hocon
task {
  consumer.parallelism    = 1
  extract.parallelism     = 2
  chunking.parallelism    = 2
  embedding.parallelism   = 2   # bottleneck — scales with API rate limit
  quantization.parallelism = 2
  sink.parallelism        = 1
}
```

The `embedding.parallelism` is typically the bottleneck. Scale it up alongside
the OpenAI API tier rate limit. Each slot maintains its own event buffer and timer,
so higher parallelism = more concurrent batch API calls.

`sink.parallelism` must stay at `1` — `BatchedOpenSearchSinkFunction` keys all docs
to a constant key (`0`) so a single slot holds the buffer and timer. Running more than
one slot would split docs across multiple isolated buffers each writing separately.
