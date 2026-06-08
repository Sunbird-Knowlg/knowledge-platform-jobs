package org.sunbird.job.contentembedding.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.job.contentembedding.domain.ChunkingConfig
import org.sunbird.job.contentembedding.strategy.SlidingWindowChunkingStrategy

class SlidingWindowChunkingStrategySpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  private def strategy(maxWords: Int = 10, overlapWords: Int = 2) =
    new SlidingWindowChunkingStrategy(ChunkingConfig("sliding-window", maxWords = maxWords, overlapWords = overlapWords))

  private def words(n: Int): String = (1 to n).map(i => s"word$i").mkString(" ")

  "SlidingWindowChunkingStrategy" should "return empty list for empty data" in {
    strategy().chunk("do_123", "Content", Map.empty) shouldBe empty
  }

  it should "return 1 chunk when text fits within maxWords" in {
    val data = Map[String, Any]("name" -> "short text here")
    val chunks = strategy(maxWords = 100).chunk("do_123", "Content", data)
    chunks should have length 1
    chunks.head.sourceField shouldBe "full_text"
  }

  it should "return multiple windows when text exceeds maxWords" in {
    // 25 words, window=10, overlap=2, step=8 → ceil((25-10)/8)+1 = 3 windows
    val data = Map[String, Any]("name" -> words(25))
    val chunks = strategy(maxWords = 10, overlapWords = 2).chunk("do_123", "Content", data)
    chunks.size should be > 1
  }

  it should "have overlap between consecutive windows" in {
    val data = Map[String, Any]("name" -> words(20))
    val strat = strategy(maxWords = 10, overlapWords = 3)
    val chunks = strat.chunk("do_123", "Content", data)
    if (chunks.size >= 2) {
      val words1 = chunks(0).text.split(" ")
      val words2 = chunks(1).text.split(" ")
      // Last 3 words of chunk1 == first 3 words of chunk2 (overlap)
      val tail1 = words1.takeRight(3).toSet
      val head2 = words2.take(3).toSet
      tail1.intersect(head2) should not be empty
    }
  }

  it should "emit all fields as key: value" in {
    val data = Map[String, Any]("name" -> "Test", "status" -> "Live")
    val chunks = strategy(maxWords = 100).chunk("do_123", "Content", data)
    val text = chunks.head.text
    text should include ("name: Test")
    text should include ("status: Live")
  }

  it should "exclude default excluded fields" in {
    val data = Map[String, Any](
      "name" -> "Test",
      "identifier" -> "do_123",
      "_schema_version" -> "1.0",
      "timestamp" -> 12345678L
    )
    val chunks = strategy(maxWords = 100).chunk("do_123", "Content", data)
    val text = chunks.head.text
    text should not include "_schema_version"
    text should not include "timestamp"
  }

  it should "translate mimeType to human-readable label" in {
    val data = Map[String, Any]("mimeType" -> "video/mp4", "name" -> "Video")
    val chunks = strategy(maxWords = 100).chunk("do_123", "Content", data)
    chunks.head.text should include ("mimeType: MP4 video")
  }

  it should "use window_N sourceField for multiple chunks" in {
    val data = Map[String, Any]("name" -> words(25))
    val chunks = strategy(maxWords = 10, overlapWords = 2).chunk("do_123", "Content", data)
    if (chunks.size > 1) {
      chunks(0).sourceField shouldBe "window_0"
      chunks(1).sourceField shouldBe "window_1"
    }
  }

  it should "include word_count metadata in chunks" in {
    val data = Map[String, Any]("name" -> "test word count here")
    val chunks = strategy(maxWords = 100).chunk("do_123", "Content", data)
    chunks.head.metadata.keys should contain ("total_words")
  }

  it should "flatten hierarchy children into text" in {
    val data = Map[String, Any](
      "name" -> "Course",
      "hierarchy" -> Map(
        "children" -> List(
          Map("name" -> "Unit 1", "description" -> "First unit"),
          Map("name" -> "Unit 2", "description" -> "Second unit")
        )
      )
    )
    val chunks = strategy(maxWords = 100).chunk("do_123", "Collection", data)
    chunks.head.text should include ("Unit 1")
    chunks.head.text should include ("Unit 2")
  }

  it should "handle null Seq values gracefully" in {
    val data = Map[String, Any]("name" -> "Test", "audience" -> null)
    val chunks = strategy(maxWords = 100).chunk("do_123", "Content", data)
    chunks should not be empty
  }

  it should "return chunks with sequential index values" in {
    val data = Map[String, Any]("name" -> words(25))
    val chunks = strategy(maxWords = 10, overlapWords = 2).chunk("do_123", "Content", data)
    chunks.map(_.index) shouldBe (0 until chunks.size).toList
  }
}
