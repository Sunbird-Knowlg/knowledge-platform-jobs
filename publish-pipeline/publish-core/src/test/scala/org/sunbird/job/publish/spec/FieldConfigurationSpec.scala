package org.sunbird.job.publish.spec

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.job.publish.helpers.FieldConfiguration

class FieldConfigurationSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  private def makeConfig(hocon: String): FieldConfiguration = {
    new FieldConfiguration(ConfigFactory.parseString(hocon))
  }

  private val baseConfig = """
    enriched {
      metadata {
        global {
          includeFields = ["identifier", "name", "status", "mimeType"]
          autoIncludeSearchEnrichmentFields = true
        }
        content {
          includeFields = ["contentType", "resourceType"]
        }
        collection {
          includeFields = ["childNodes"]
          includeHierarchy = true
        }
        question {
          includeFields = []
        }
        questionset {
          includeFields = ["totalQuestions"]
          includeHierarchy = true
        }
      }
    }
  """

  "FieldConfiguration" should "return global fields for unknown type" in {
    val fc = makeConfig(baseConfig)
    val fields = fc.getFieldNamesFor("unknown")
    fields should contain allOf ("identifier", "name", "status", "mimeType")
  }

  it should "merge global + type-specific fields for Content" in {
    val fc = makeConfig(baseConfig)
    val fields = fc.getFieldNamesFor("Content")
    fields should contain allOf ("identifier", "name", "contentType", "resourceType")
  }

  it should "merge global + type-specific fields for Collection" in {
    val fc = makeConfig(baseConfig)
    val fields = fc.getFieldNamesFor("Collection")
    fields should contain allOf ("identifier", "name", "childNodes")
  }

  it should "be case-insensitive for objectType lookup" in {
    val fc = makeConfig(baseConfig)
    fc.getFieldNamesFor("CONTENT") should contain ("contentType")
    fc.getFieldNamesFor("content") should contain ("contentType")
    fc.getFieldNamesFor("Content") should contain ("contentType")
  }

  it should "return no duplicates when global and type-specific overlap" in {
    val fc = makeConfig("""
      enriched.metadata.global.includeFields = ["identifier", "name"]
      enriched.metadata.content.includeFields = ["identifier", "contentType"]
    """)
    val fields = fc.getFieldNamesFor("Content")
    fields.count(_ == "identifier") shouldBe 1
  }

  it should "return shouldIncludeHierarchy=true for Collection" in {
    val fc = makeConfig(baseConfig)
    fc.shouldIncludeHierarchy("Collection") shouldBe true
    fc.shouldIncludeHierarchy("collection") shouldBe true
  }

  it should "return shouldIncludeHierarchy=true for QuestionSet" in {
    val fc = makeConfig(baseConfig)
    fc.shouldIncludeHierarchy("QuestionSet") shouldBe true
  }

  it should "return shouldIncludeHierarchy=false for Content" in {
    val fc = makeConfig(baseConfig)
    fc.shouldIncludeHierarchy("Content") shouldBe false
  }

  it should "return shouldIncludeHierarchy=false for missing config" in {
    val fc = makeConfig("enriched.metadata.global.includeFields = []")
    fc.shouldIncludeHierarchy("Collection") shouldBe false
  }

  it should "return shouldAutoIncludeSearchEnrichmentFields=true when configured" in {
    val fc = makeConfig(baseConfig)
    fc.shouldAutoIncludeSearchEnrichmentFields shouldBe true
  }

  it should "return shouldAutoIncludeSearchEnrichmentFields=false when not configured" in {
    val fc = makeConfig("enriched.metadata.global.includeFields = []")
    fc.shouldAutoIncludeSearchEnrichmentFields shouldBe false
  }

  it should "identify se_* fields correctly" in {
    val fc = makeConfig(baseConfig)
    fc.isSearchEnrichmentField("se_boards") shouldBe true
    fc.isSearchEnrichmentField("se_subjects") shouldBe true
    fc.isSearchEnrichmentField("name") shouldBe false
    fc.isSearchEnrichmentField("status") shouldBe false
  }

  it should "return empty fields gracefully when config is missing" in {
    val fc = makeConfig("")
    fc.getFieldNamesFor("Content") shouldBe empty
  }

  it should "list all configured object types" in {
    val fc = makeConfig(baseConfig)
    val types = fc.getAllConfiguredObjectTypes
    types should contain allOf ("content", "collection", "question", "questionset")
  }
}
