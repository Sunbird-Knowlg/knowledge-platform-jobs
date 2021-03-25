package org.sunbird.job.spec

import java.util

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.task.{QuestionSetPublishConfig, QuestionSetPublishStreamTask}
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.publish.domain.Event
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}
import org.sunbird.publish.config.PublishConfig
import org.sunbird.publish.util.CloudStorageUtil
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

class QuestionSetPublishStreamTaskSpec extends BaseTestSpec {

	implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
	implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

	val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
	  .setConfiguration(testConfiguration())
	  .setNumberSlotsPerTaskManager(1)
	  .setNumberTaskManagers(1)
	  .build)
	val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
	val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
	implicit val jobConfig: QuestionSetPublishConfig = new QuestionSetPublishConfig(config)

	val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
	val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
	var cassandraUtil: CassandraUtil = _
	val publishConfig: PublishConfig = new PublishConfig(config, "")
	val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)

	override protected def beforeAll(): Unit = {
		super.beforeAll()
		EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
		cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
		val session = cassandraUtil.session
		val dataLoader = new CQLDataLoader(session)
		dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
		flinkCluster.before()
	}

	override protected def afterAll(): Unit = {
		super.afterAll()
		try {
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
		} catch {
			case ex: Exception => {
			}
		}
		flinkCluster.after()
	}

	//TODO: provide test cases.
	def initialize(): Unit = {
		when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new QuestionPublishEventSource)
	}

	ignore should " publish the question " in {
		initialize
		new QuestionSetPublishStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
		BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
		BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.questionPublishEventCount}").getValue() should be(1)
	}
}

private class QuestionPublishEventSource extends SourceFunction[Event] {

	override def run(ctx: SourceContext[Event]) {
		ctx.collect(jsonToEvent(EventFixture.QUESTION_EVENT1))
	}

	override def cancel() = {}

	def jsonToEvent(json: String): Event = {
		val gson = new Gson()
		val data = gson.fromJson(json, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]]
		val metadataMap = data.get("edata").asInstanceOf[util.Map[String, Any]].get("metadata").asInstanceOf[util.Map[String, Any]]
		metadataMap.put("pkgVersion",metadataMap.get("pkgVersion").asInstanceOf[Double].toInt)
		new Event(data)
	}

}
