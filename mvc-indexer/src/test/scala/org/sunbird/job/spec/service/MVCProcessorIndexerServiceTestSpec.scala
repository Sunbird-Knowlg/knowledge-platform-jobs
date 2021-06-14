package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.Mockito
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.mvcindexer.functions.MVCIndexer
import org.sunbird.job.mvcindexer.service.MVCIndexerService
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil, HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

import java.util

class MVCProcessorIndexerServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: MVCIndexerConfig = new MVCIndexerConfig(config)
  val mockElasticUtil:ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  val httpUtil: HttpUtil = new HttpUtil
  lazy val mvcProcessorIndexer: MVCIndexerService = new MVCIndexerService(jobConfig, mockElasticUtil,httpUtil)

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    val cassandraUtil = new CassandraUtil(jobConfig.lmsDbHost, jobConfig.lmsDbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "MVCProcessorIndexerService" should "generate es log" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)

    mvcProcessorIndexer.processMessage(inputEvent)
  }

  "MVCProcessorIndexerService" should "generate cassandra log" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 11)
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }
}
