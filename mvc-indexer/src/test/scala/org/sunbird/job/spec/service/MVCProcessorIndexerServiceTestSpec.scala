package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.Mockito
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.MVCProcessorIndexer
import org.sunbird.job.task.MVCProcessorIndexerConfig
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

import java.util

class MVCProcessorIndexerServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: MVCProcessorIndexerConfig = new MVCProcessorIndexerConfig(config)
  val mockElasticUtil:ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  lazy val mvcProcessorIndexer: MVCProcessorIndexer = new MVCProcessorIndexer(jobConfig, mockElasticUtil)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "MVCProcessorIndexerService" should "generate es log" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)

  }

  "MVCProcessorIndexerService" should "generate cassandra log" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 11)
  }
}
