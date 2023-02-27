package org.sunbird.job.util

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.qrimagegenerator.task.QRCodeImageGeneratorConfig
import org.sunbird.job.qrimagegenerator.util.QRCodeImageGeneratorUtil

class QRCodeImageGeneratorUtilSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: QRCodeImageGeneratorConfig = new QRCodeImageGeneratorConfig(config)
  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  var cassandraUtil: CassandraUtil = _
  implicit val mockCloudUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())
  val mockElasticUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort, jobConfig)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    cassandraUtil.close()
    super.afterAll()

  }

  "QRCodeImageGeneratorFunction" should "return QR Code DOcument" in {
    val qRCodeImageGeneratorUtil = new QRCodeImageGeneratorUtil(jobConfig, cassandraUtil, mockCloudUtil, mockElasticUtil)

    val Q1I5I3Json = """{"identifier":"Q1I5I3", "filename":"0_Q1I5I3", "channel":"b00bc992ef25f1a9a8d63291e20efc8d"}"""
    when(mockElasticUtil.getDocumentAsString("Q1I5I3")).thenReturn(Q1I5I3Json)

    val indexedDocument = qRCodeImageGeneratorUtil.getIndexDocument("Q1I5I3")(mockElasticUtil)

    assert(indexedDocument.nonEmpty)
  }
}




