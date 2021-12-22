package org.sunbird.job.publish.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.publish.core.PublishMetadata
import org.sunbird.job.publish.helpers.EventGenerator
import org.sunbird.job.util.OntologyEngineContext

class EventGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar{

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "pushPublishEvent" should "push the generated event" in {
    implicit val oec = new OntologyEngineContext()

    val publishChainEvent : Map[String,AnyRef] = Map("identifier" -> "113430","mimeType" -> "application/vnd.sunbird.questionset",
      "objectType" -> "QuestionSet", "lastPublishedBy"->"","pkgVersion"->"2","state"->"Processing")
    val eData : Map[String,AnyRef]=  Map("publish_type"-> "public","action"->"publishchain","iteration"->"1")
    val context : Map[String,AnyRef] = Map("channel"->"Sunbird","pdata"->Map("id"->"org.sunbird.platform","ver"->"3.0"),"env"->"dev")
    val obj = Map("id"->"do_11342300698931200012","ver"->"1637732551528")
    val publishChainList = List(Map[String, AnyRef]())
    val publishMetadata = new PublishMetadata("",0,"",eData,context,obj,publishChainList);
    EventGenerator.pushPublishEvent(publishChainEvent,publishMetadata,"kafka.input.questionset","questionset-publish")

  }

  "pushPublishChainEvent" should "push the generated chain event" in {
    implicit val oec = new OntologyEngineContext()

    val publishChainEvent : scala.collection.mutable.Map[String,AnyRef] =  scala.collection.mutable.Map("identifier" -> "113430","mimeType" -> "application/vnd.sunbird.questionset",
      "objectType" -> "QuestionSet", "lastPublishedBy"->"","pkgVersion"->"2","state"->"Processing")
    val publishChainList : List[scala.collection.mutable.Map[String,AnyRef]] = List(scala.collection.mutable.Map("identifier" -> "113430","mimeType" -> "application/vnd.sunbird.questionset",
      "objectType" -> "QuestionSet", "lastPublishedBy"->"","pkgVersion"->"2","state"->"Processing"))
    EventGenerator.pushPublishChainEvent(publishChainEvent,publishChainList,"kafka.input.questionset")

  }

}
