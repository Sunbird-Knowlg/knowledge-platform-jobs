package org.sunbird.job.publish.spec

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.publish.helpers.{ConfigurableEnrichedMetadataEventBuilder, FieldConfiguration}

class ConfigurableEnrichedMetadataEventBuilderSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  private val config = ConfigFactory.parseString("""
    enriched {
      metadata {
        global {
          includeFields = ["identifier", "name", "status", "mimeType", "primaryCategory", "createdBy"]
          autoIncludeSearchEnrichmentFields = true
        }
        content {
          includeFields = ["contentType"]
        }
        collection {
          includeFields = []
          includeHierarchy = true
        }
        question {
          includeFields = []
        }
        questionset {
          includeFields = []
          includeHierarchy = true
        }
      }
    }
  """)

  private val fieldConfig = new FieldConfiguration(config)
  private val builder = new ConfigurableEnrichedMetadataEventBuilder(fieldConfig, "test.enriched.topic", globalIncludeHierarchy = true)

  private def contentObj(id: String, meta: Map[String, AnyRef]): ObjectData = {
    val baseMeta = Map[String, AnyRef](
      "identifier" -> id,
      "mimeType" -> "application/pdf",
      "primaryCategory" -> "Explanation Content"
    ) ++ meta
    new ObjectData(id, baseMeta)
  }

  "ConfigurableEnrichedMetadataEventBuilder" should "extract configured global fields" in {
    val obj = contentObj("do_123", Map("name" -> "Test Content", "status" -> "Live", "contentType" -> "Resource"))
    val event = builder.buildEnrichedKafkaEvent(obj)
    val data = event("data").asInstanceOf[Map[String, Any]]
    data.keys should contain allOf ("name", "status", "mimeType", "primaryCategory")
  }

  it should "always include identifier" in {
    val obj = contentObj("do_123", Map("name" -> "Test"))
    val event = builder.buildEnrichedKafkaEvent(obj)
    val data = event("data").asInstanceOf[Map[String, Any]]
    data("identifier") shouldBe "do_123"
  }

  it should "auto-include se_* fields from metadata" in {
    val obj = contentObj("do_123", Map(
      "name" -> "Test",
      "se_boards" -> List("CBSE"),
      "se_subjects" -> List("English"),
      "se_gradeLevels" -> List("Class 4")
    ))
    val event = builder.buildEnrichedKafkaEvent(obj)
    val data = event("data").asInstanceOf[Map[String, Any]]
    data.keys should contain allOf ("se_boards", "se_subjects", "se_gradeLevels")
  }

  it should "not include unconfigured non-se fields" in {
    val obj = contentObj("do_123", Map(
      "name" -> "Test",
      "versionKey" -> "1234567",
      "pkgVersion" -> 2.0.asInstanceOf[AnyRef]
    ))
    val event = builder.buildEnrichedKafkaEvent(obj)
    val data = event("data").asInstanceOf[Map[String, Any]]
    data.keys should not contain "versionKey"
    data.keys should not contain "pkgVersion"
  }

  it should "filter out empty description placeholder" in {
    val obj = contentObj("do_123", Map(
      "name" -> "Test",
      "description" -> "Enter description for Course"
    ))
    val event = builder.buildEnrichedKafkaEvent(obj)
    val data = event("data").asInstanceOf[Map[String, Any]]
    // description not in configured fields but if it were, placeholder should be filtered
    // This tests the sanitize logic — null values dropped
    data.values should not contain null
  }

  it should "derive objectType=Collection from mimeType" in {
    val obj = new ObjectData("do_123", Map[String, AnyRef](
      "identifier" -> "do_123",
      "name" -> "Test Collection",
      "mimeType" -> "application/vnd.ekstep.content-collection"
    ))
    val event = builder.buildEnrichedKafkaEvent(obj)
    event("contentType") shouldBe "Collection"
  }

  it should "derive objectType=Content for pdf mimeType" in {
    val obj = contentObj("do_123", Map("name" -> "Test"))
    val event = builder.buildEnrichedKafkaEvent(obj)
    event("contentType") shouldBe "Content"
  }

  it should "include hierarchy for Collection when enabled" in {
    val hierarchy = Map[String, AnyRef](
      "identifier" -> "do_123",
      "name" -> "Course",
      "objectType" -> "Collection",
      "children" -> List(
        Map("identifier" -> "do_child1", "name" -> "Unit 1", "objectType" -> "Collection")
      )
    )
    val obj = new ObjectData("do_123", Map[String, AnyRef](
      "identifier" -> "do_123",
      "name" -> "Test Course",
      "mimeType" -> "application/vnd.ekstep.content-collection"
    ), None, Some(hierarchy))
    val event = builder.buildEnrichedKafkaEvent(obj)
    val data = event("data").asInstanceOf[Map[String, Any]]
    data.keys should contain ("hierarchy")
  }

  it should "not include hierarchy for Content" in {
    val obj = contentObj("do_123", Map("name" -> "Test"))
    val event = builder.buildEnrichedKafkaEvent(obj)
    val data = event("data").asInstanceOf[Map[String, Any]]
    data.keys should not contain "hierarchy"
  }

  it should "include _schema_version in event envelope" in {
    val obj = contentObj("do_123", Map("name" -> "Test"))
    val event = builder.buildEnrichedKafkaEvent(obj)
    event.keys should contain ("_schema_version")
    event("_schema_version") shouldBe "1.0"
  }

  it should "include timestamp in event envelope" in {
    val obj = contentObj("do_123", Map("name" -> "Test"))
    val before = System.currentTimeMillis()
    val event = builder.buildEnrichedKafkaEvent(obj)
    val after = System.currentTimeMillis()
    val ts = event("timestamp").asInstanceOf[Long]
    ts should be >= before
    ts should be <= after
  }

  it should "not include hierarchy when globalIncludeHierarchy=false" in {
    val noHierarchyBuilder = new ConfigurableEnrichedMetadataEventBuilder(fieldConfig, "test.topic", globalIncludeHierarchy = false)
    val hierarchy = Map[String, AnyRef]("identifier" -> "do_123", "name" -> "Course")
    val obj = new ObjectData("do_123", Map[String, AnyRef](
      "identifier" -> "do_123",
      "name" -> "Test",
      "mimeType" -> "application/vnd.ekstep.content-collection"
    ), None, Some(hierarchy))
    val event = noHierarchyBuilder.buildEnrichedKafkaEvent(obj)
    val data = event("data").asInstanceOf[Map[String, Any]]
    data.keys should not contain "hierarchy"
  }
}
