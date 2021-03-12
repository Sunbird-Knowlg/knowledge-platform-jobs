package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.util.DefinitionCache

class DefinitionCacheTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf")
  val definitionCache = new DummyDefinitionCache()
  val basePath = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"

  "DefinitionCache" should "return the definition for the objectType and version specified " in {
    val definition = definitionCache.getDefinition("collection", "1.0", basePath)
    definition.isEmpty should be(false)
    val title = definition.getOrElse("schema", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("title", "").asInstanceOf[String]
    title should be("Collection")
    val objectType = definition.getOrElse("config", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("objectType", "").asInstanceOf[String]
    objectType should be("Collection")
  }

  it should "return the definition for the object type" in {
    val definition = definitionCache.getDefinition("Collection", "1.0", basePath)
    val schema = definition.getOrElse("schema", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val config = definition.getOrElse("config", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    schema.isEmpty should be(false)
    config.isEmpty should be(false)
    config.getOrElse("objectType", "").asInstanceOf[String] should be("Collection")
  }
}

class DummyDefinitionCache extends DefinitionCache {}
