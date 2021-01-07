package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}
import org.sunbird.spec.BaseTestSpec

class QuestionSetPublishStreamTaskSpec extends BaseTestSpec {

	implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
	implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

	val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
	  .setConfiguration(testConfiguration())
	  .setNumberSlotsPerTaskManager(1)
	  .setNumberTaskManagers(1)
	  .build)
	val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
	val config: Config = ConfigFactory.load("test.conf")
	implicit val jobConfig: QuestionSetPublishConfig = new QuestionSetPublishConfig(config)

	implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
	val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
	var cassandraUtil: CassandraUtil = _

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


}
