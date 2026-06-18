package org.sunbird.job.contentembedding.spec

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.job.contentembedding.domain.{ChunkedEvent, TextChunk}
import org.sunbird.job.contentembedding.task.ContentEmbeddingConfig

class BatchEmbeddingFunctionSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  private val config: ContentEmbeddingConfig = {
    val raw = ConfigFactory.load("test.conf")
    new ContentEmbeddingConfig(raw)
  }

  private def makeChunk(text: String, idx: Int): TextChunk =
    TextChunk(text = text, sourceField = "metadata", index = idx)

  private def makeEvent(id: String, texts: String*): ChunkedEvent =
    ChunkedEvent(
      objectId      = id,
      contentType   = "Content",
      schemaVersion = "1.0",
      chunks        = texts.zipWithIndex.map { case (t, i) => makeChunk(t, i) }.toList
    )

  // --- Config reading ---

  "ContentEmbeddingConfig" should "read batch_events from test.conf" in {
    config.embeddingBatchEvents shouldBe 3
  }

  it should "read window_size_ms from test.conf" in {
    config.embeddingWindowSizeMs shouldBe 1000L
  }

  it should "read opensearch bulk.size from test.conf" in {
    config.osBulkSize shouldBe 5
  }

  it should "read opensearch bulk.flush_interval_ms from test.conf" in {
    config.osBulkFlushIntervalMs shouldBe 1000L
  }

  // --- Chunk redistribution logic ---

  "Batch vector redistribution" should "correctly split a flat vector list back to per-event slices" in {
    val event1 = makeEvent("do_1", "chunk A", "chunk B")
    val event2 = makeEvent("do_2", "chunk C")
    val event3 = makeEvent("do_3", "chunk D", "chunk E", "chunk F")

    val events = List(event1, event2, event3)
    val chunkCounts = events.map(_.chunks.size)

    chunkCounts shouldBe List(2, 1, 3)

    // Simulate flat embedding list
    val totalChunks = chunkCounts.sum
    val fakeVectors: List[Array[Double]] = (0 until totalChunks).map(i => Array(i.toDouble)).toList

    var offset = 0
    val splitVectors = events.zip(chunkCounts).map { case (event, count) =>
      val slice = fakeVectors.slice(offset, offset + count)
      offset += count
      (event.objectId, slice)
    }

    splitVectors should have size 3
    splitVectors.find(_._1 == "do_1").get._2 should have size 2
    splitVectors.find(_._1 == "do_2").get._2 should have size 1
    splitVectors.find(_._1 == "do_3").get._2 should have size 3

    // Verify no cross-contamination between events
    splitVectors.find(_._1 == "do_1").get._2.map(_(0)) shouldBe List(0.0, 1.0)
    splitVectors.find(_._1 == "do_2").get._2.map(_(0)) shouldBe List(2.0)
    splitVectors.find(_._1 == "do_3").get._2.map(_(0)) shouldBe List(3.0, 4.0, 5.0)
  }

  it should "handle a single event with multiple chunks" in {
    val event = makeEvent("do_single", "t0", "t1", "t2", "t3")
    val fakeVectors = event.chunks.indices.map(i => Array(i.toDouble)).toList

    var offset = 0
    val slices = List(event).zip(List(event.chunks.size)).map { case (e, count) =>
      val s = fakeVectors.slice(offset, offset + count)
      offset += count
      s
    }

    slices.head should have size 4
    offset shouldBe 4
  }

  it should "handle multiple events each with a single chunk" in {
    val events = (1 to 5).map(i => makeEvent(s"do_$i", s"text $i")).toList
    val chunkCounts = events.map(_.chunks.size)

    chunkCounts should contain only 1
    chunkCounts.sum shouldBe 5
  }

  // --- Key bucketing ---

  "Embedding key assignment" should "assign events to buckets within [0, parallelism)" in {
    val parallelism = config.embeddingParallelism
    val events = (1 to 50).map(i => makeEvent(s"do_$i", "text")).toList

    val keys = events.map(e => Math.abs(e.objectId.hashCode) % parallelism)
    keys.foreach { k =>
      k should be >= 0
      k should be < parallelism
    }
  }

  it should "distribute events across all buckets for sufficient volume" in {
    val parallelism = 4
    val events = (1 to 200).map(i => makeEvent(s"do_unique_$i", "text")).toList
    val keys = events.map(e => Math.abs(e.objectId.hashCode) % parallelism).toSet
    keys.size should be > 1
  }
}
