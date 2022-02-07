package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.junit.Assert.assertNotNull
import org.mockito.{ArgumentMatchers, Mockito}
import org.sunbird.job.Metrics
import org.sunbird.job.content.function.InteractiveContentFunction
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.content.task.InteractiveContentPublishConfig
import org.sunbird.job.exception.KafkaClientException
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.util.{JSONUtil, KafkaClientUtil}
import org.sunbird.spec.BaseTestSpec

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class PublishChainFunctionSpec extends BaseTestSpec {
  val mockKafkaClientUtil: KafkaClientUtil = mock[KafkaClientUtil](Mockito.withSettings().serializable())

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: InteractiveContentPublishConfig = new InteractiveContentPublishConfig(config)
  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {
    {
      put(jobConfig.publishChainEventCount, new AtomicLong())
      put(jobConfig.publishChainSuccessEventCount, new AtomicLong())
      put(jobConfig.publishChainFailedEventCount, new AtomicLong())
    }
  })
  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "publishChainForQuestionSet" should "throws exception for invalid topic" in {
    Mockito.when(mockKafkaClientUtil.send(ArgumentMatchers.anyString(),ArgumentMatchers.anyString())).thenThrow(new KafkaClientException("Topic does not exists."))
    assertThrows[Exception] {
      val event = new InteractiveContentFunction(jobConfig)(stringTypeInfo).processElement(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.PUBLISH_CHAIN_EVENT),0,0), null, metrics)
    }
  }


  "publishChainForInvalidObjectType" should "should do nothing" in {
    val event = new InteractiveContentFunction(jobConfig)(stringTypeInfo).processElement(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.PUBLISH_CHAIN_EVENT_WITH_INVALID_TYPE),0,0),null,metrics)
    assertNotNull(event)
  }
}


