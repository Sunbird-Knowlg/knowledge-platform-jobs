package org.sunbird.job.util

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.task.CompositeSearchIndexerConfig

class DefinitionUtilTest extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig = new CompositeSearchIndexerConfig(config)

  "getDefinition" should "return the definition for the objectType and version specified " in {
    val definitionUtility = new DefinitionUtil
    val definition = definitionUtility.getDefinition("collection", "1.0", jobConfig.definitionBasePath)
    definition.isEmpty should be(false)
    val title = definition.getOrElse("schema", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("title", "").asInstanceOf[String]
    title should be("Collection")
    val objectType = definition.getOrElse("config", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("objectType", "").asInstanceOf[String]
    objectType should be("Collection")
  }
}
