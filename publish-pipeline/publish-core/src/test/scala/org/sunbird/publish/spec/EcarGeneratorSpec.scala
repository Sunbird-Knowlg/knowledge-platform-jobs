package org.sunbird.publish.spec

import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.publish.config.PublishConfig
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.helpers.EcarGenerator
import org.sunbird.publish.util.CloudStorageUtil

class EcarGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)

  "Object Ecar Generator generateEcar" should "return a Map containing Packaging Type and its url after uploading it to cloud" in {

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("identifier" -> "do_123", "appIcon" -> "https://dev.sunbirded.org/assets/images/sunbird_logo.png", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuesstionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val obj = new TestEcarGenerator()
    val enObjects: List[Map[String, AnyRef]] = obj.getDataForEcar(objData).getOrElse(List())
    val result = obj.generateEcar(objData, enObjects,"SPINE")
    result.isEmpty should be(false)
  }
}

class TestEcarGenerator extends EcarGenerator {

  val testObj = List(Map("children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")), "name" -> "Test QuestionSet", "appIcon" -> "https://dev.sunbirded.org/assets/images/sunbird_logo.png", "IL_FUNC_OBJECT_TYPE" -> "QuesstionSet", "identifier" -> "do_123", "status" -> "Live", "IL_UNIQUE_ID" -> "do_123"), Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2"))
  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = Some(testObj)
}