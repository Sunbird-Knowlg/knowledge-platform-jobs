package org.sunbird.job.contentembedding.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.job.contentembedding.domain.ChunkingConfig
import org.sunbird.job.contentembedding.strategy.SemanticChunkingStrategy

class SemanticChunkingStrategySpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  private val strategy = new SemanticChunkingStrategy()

  private def data(fields: (String, Any)*): Map[String, Any] = Map(fields: _*)

  "SemanticChunkingStrategy" should "return empty list for empty data" in {
    strategy.chunk("do_123", "Content", Map.empty) shouldBe empty
  }

  it should "return 1 chunk for Content" in {
    val d = data("name" -> "Test Content", "status" -> "Live", "primaryCategory" -> "Learning Resource")
    val chunks = strategy.chunk("do_123", "Content", d)
    chunks should have length 1
  }

  it should "emit all fields as key: value format" in {
    val d = data("name" -> "Test Content", "status" -> "Live")
    val chunks = strategy.chunk("do_123", "Content", d)
    val text = chunks.head.text
    text should include ("name: Test Content")
    text should include ("status: Live")
  }

  it should "translate mimeType to human-readable label" in {
    val d = data("name" -> "Test", "mimeType" -> "application/pdf")
    val chunks = strategy.chunk("do_123", "Content", d)
    chunks.head.text should include ("mimeType: PDF document")
    chunks.head.text should not include "application/pdf"
  }

  it should "exclude default excluded fields from chunk text" in {
    val d = data(
      "name" -> "Test",
      "identifier" -> "do_123",
      "_schema_version" -> "1.0",
      "timestamp" -> 12345678L,
      "hierarchy" -> Map("children" -> List())
    )
    val chunks = strategy.chunk("do_123", "Content", d)
    val text = chunks.head.text
    text should not include "identifier"
    text should not include "_schema_version"
    text should not include "timestamp"
    text should not include "hierarchy"
  }

  it should "handle List values by joining with comma" in {
    val d = data("audience" -> List("Student", "Teacher"))
    val chunks = strategy.chunk("do_123", "Content", d)
    chunks.head.text should include ("Student, Teacher")
  }

  it should "truncate chunk text to maxChunkSize" in {
    val longValue = "x" * 2000
    val d = data("name" -> longValue)
    val config = ChunkingConfig("semantic", maxChunkSize = 100)
    val strat = new SemanticChunkingStrategy(config)
    val chunks = strat.chunk("do_123", "Content", d)
    chunks.head.text.length should be <= 100
  }

  it should "return 1 chunk for Question" in {
    val d = data("name" -> "Q1", "body" -> "What is photosynthesis?")
    val chunks = strategy.chunk("do_123", "Question", d)
    chunks should have length 1
    chunks.head.sourceField shouldBe "question_full"
  }

  it should "return metadata chunk + child chunks for Collection with hierarchy" in {
    val d = data(
      "name" -> "Course",
      "hierarchy" -> Map(
        "identifier" -> "do_123",
        "children" -> List(
          Map("identifier" -> "do_child1", "name" -> "Unit 1", "description" -> "First unit"),
          Map("identifier" -> "do_child2", "name" -> "Unit 2", "description" -> "Second unit")
        )
      )
    )
    val chunks = strategy.chunk("do_123", "Collection", d)
    chunks.size should be >= 3 // 1 metadata + 2 children
    chunks.head.sourceField shouldBe "collection_metadata"
    chunks.map(_.sourceField) should contain ("child_do_child1")
    chunks.map(_.sourceField) should contain ("child_do_child2")
  }

  it should "protect against circular hierarchy references" in {
    // Child references itself via nested children — should not infinite loop
    val d = data(
      "name" -> "Course",
      "hierarchy" -> Map(
        "identifier" -> "do_123",
        "children" -> List(
          Map("identifier" -> "do_child1", "name" -> "Unit 1",
            "children" -> List(
              Map("identifier" -> "do_child1", "name" -> "Unit 1 (circular)")
            ))
        )
      )
    )
    // Should complete without StackOverflow
    val chunks = strategy.chunk("do_123", "Collection", d)
    chunks should not be empty
  }

  it should "return collection_metadata sourceField for Collection chunk" in {
    val d = data("name" -> "My Course")
    val chunks = strategy.chunk("do_123", "Collection", d)
    chunks.head.sourceField shouldBe "collection_metadata"
  }

  it should "return questionset_metadata sourceField for QuestionSet" in {
    val d = data("name" -> "My QSet")
    val chunks = strategy.chunk("do_123", "QuestionSet", d)
    chunks.head.sourceField shouldBe "questionset_metadata"
  }

  it should "return chunk with index=0 for first chunk" in {
    val d = data("name" -> "Test")
    val chunks = strategy.chunk("do_123", "Content", d)
    chunks.head.index shouldBe 0
  }

  it should "handle null values gracefully" in {
    val d = Map[String, Any]("name" -> "Test", "description" -> null)
    // Should not throw
    val chunks = strategy.chunk("do_123", "Content", d)
    chunks should not be empty
  }
}
