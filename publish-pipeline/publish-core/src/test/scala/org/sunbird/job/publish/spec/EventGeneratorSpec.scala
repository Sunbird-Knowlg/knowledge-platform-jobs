package org.sunbird.job.publish.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito.when
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.exception.KafkaClientException
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.publish.helpers.EventGenerator
import org.sunbird.job.util.{JSONUtil, KafkaClientUtil, OntologyEngineContext}

import java.util
import scala.collection.mutable

class EventGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar{

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val mockKafkaClientUtil: KafkaClientUtil = mock[KafkaClientUtil](Mockito.withSettings().serializable())



  "pushPublishEvent with invalid topic" should "throws exception" in {
    Mockito.reset(mockKafkaClientUtil)
    val topic ="";
    implicit val oec = new OntologyEngineContext()
    when(mockKafkaClientUtil.send(ArgumentMatchers.anyString(),ArgumentMatchers.anyString())).thenThrow(new KafkaClientException( "Topic with name: " + topic + ", does not exists."))
    val publishChainEvent : Map[String,AnyRef] = Map("identifier" -> "113430","mimeType" -> "application/vnd.sunbird.questionset",
      "objectType" -> "QuestionSet", "lastPublishedBy"->"","pkgVersion"->"2","state"->"Processing")
    assertThrows[Exception] {
      EventGenerator.pushPublishEvent(publishChainEvent,JSONUtil.deserialize[mutable.Map[String, Any]](EventFixture.PUBLISH_CHAIN_EVENT),topic,"questionset-publish")
    }
  }


  "pushPublishChainEvent with invalid topic" should "throws exception" in {
    Mockito.reset(mockKafkaClientUtil)
    val topic ="test";
    implicit val oec = new OntologyEngineContext()
    when(mockKafkaClientUtil.send(ArgumentMatchers.anyString(),ArgumentMatchers.anyString())).thenThrow(new KafkaClientException("Topic with name: " + topic + ", does not exists."))

    val publishChainEvent : scala.collection.mutable.Map[String,AnyRef] =  scala.collection.mutable.Map("identifier" -> "113430","mimeType" -> "application/vnd.sunbird.questionset",
      "objectType" -> "QuestionSet", "lastPublishedBy"->"","pkgVersion"->"2","state"->"Processing")
    val publishChainList : List[scala.collection.mutable.Map[String,AnyRef]] = List(scala.collection.mutable.Map("identifier" -> "113430","mimeType" -> "application/vnd.sunbird.questionset",
      "objectType" -> "QuestionSet", "lastPublishedBy"->"","pkgVersion"->"2","state"->"Processing"))
    assertThrows[Exception] {
      EventGenerator.pushPublishChainEvent(publishChainEvent, publishChainList, topic)
    }
  }

  "logInstructionEvent with invalid json" should "throws exception" in {
    val context = new util.HashMap[String, Any]()
    val obj = new util.HashMap[String, Any]()
    val actor = new util.HashMap[String, Any]()

    assertThrows[Exception] {
      EventGenerator.logInstructionEvent(actor, context, obj,null)
    }
  }
}
