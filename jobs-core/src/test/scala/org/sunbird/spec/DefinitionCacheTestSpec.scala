package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.DefinitionCache

class DefinitionCacheTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf")
  val definitionCache = new DefinitionCache()
  val basePath = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"

  "DefinitionCache" should "return the definition for the objectType and version specified " in {
    val definition = definitionCache.getDefinition("collection", "1.0", basePath)
    definition should not be(null)

    val title = definition.schema.getOrElse("title", "").asInstanceOf[String]
    title should be("Collection")
    val objectType = definition.config.getOrElse("objectType", "").asInstanceOf[String]
    objectType should be("Collection")
  }

  it should "return the definition for the object type" in {
    val definition = definitionCache.getDefinition("Collection", "1.0", basePath)
    val schema = definition.schema
    val config = definition.config
    schema.isEmpty should be(false)
    config.isEmpty should be(false)
    config.getOrElse("objectType", "").asInstanceOf[String] should be("Collection")
  }

  it should "return relation labels" in {
    val definition = definitionCache.getDefinition("Collection", "1.0", basePath)
    definition.relationLabel("Content", "IN", "hasSequenceMember") should be(Some("collections"))
    definition.relationLabel("ContentImage", "OUT", "hasSequenceMember") should be(Some("children"))
  }

  it should "return external properties"  in {
    val definition = definitionCache.getDefinition("Collection", "1.0", basePath)
    definition.externalProperties.size should be >= 1
    definition.externalProperties contains("hierarchy")
  }
}
