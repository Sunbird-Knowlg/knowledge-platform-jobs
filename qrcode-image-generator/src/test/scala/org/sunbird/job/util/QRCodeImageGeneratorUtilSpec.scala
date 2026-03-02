package org.sunbird.job.util

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.anyString
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.qrimagegenerator.task.QRCodeImageGeneratorConfig
import org.sunbird.job.qrimagegenerator.util.QRCodeImageGeneratorUtil
import com.datastax.driver.core.Row

class QRCodeImageGeneratorUtilSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: QRCodeImageGeneratorConfig = new QRCodeImageGeneratorConfig(config)
  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  implicit val mockCloudUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())
  val mockElasticUtil: ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "QRCodeImageGeneratorFunction" should "return QR Code Document" in {
    val qrCodeImageGeneratorUtil = new QRCodeImageGeneratorUtil(jobConfig, mockCassandraUtil, mockCloudUtil, mockElasticUtil)
    
    when(mockCassandraUtil.findOne(anyString())).thenReturn(null)
    
    assertThrows[InvalidInputException] {
      qrCodeImageGeneratorUtil.indexImageInDocument("Q1I5I3")(mockElasticUtil, mockCassandraUtil)
    }
    
    val Q1I5I3Json = """{"identifier":"Q1I5I3", "filename":"0_Q1I5I3", "channel":"b00bc992ef25f1a9a8d63291e20efc8d"}"""
    when(mockElasticUtil.getDocumentAsString("Q1I5I3")).thenReturn(Q1I5I3Json)
    
    val mockRow = mock[Row]
    when(mockRow.getString("image_url")).thenReturn("http://test.com/qr.png")
    when(mockCassandraUtil.findOne(anyString())).thenReturn(mockRow)
    
    qrCodeImageGeneratorUtil.indexImageInDocument("Q1I5I3")(mockElasticUtil, mockCassandraUtil)
  }
}
