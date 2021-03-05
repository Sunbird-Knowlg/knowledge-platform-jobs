package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.util.DefinitionUtil

class DefinitionUtilTest extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf")

  "getDefinition" should "return the definition for the objectType and version specified " in {
    val definition = DefinitionUtil.get("collection", "1.0", "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local")
    definition.isEmpty should be(false)
    val title = definition.getOrElse("schema", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("title", "").asInstanceOf[String]
    title should be("Collection")
    val objectType = definition.getOrElse("config", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("objectType", "").asInstanceOf[String]
    objectType should be("Collection")
  }
}
