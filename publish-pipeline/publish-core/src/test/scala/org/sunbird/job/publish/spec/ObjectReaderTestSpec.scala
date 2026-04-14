package org.sunbird.job.publish.spec

import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{ExtDataConfig, ObjectExtData}
import org.sunbird.job.publish.helpers.ObjectReader
import org.sunbird.job.util.{CassandraUtil, JanusGraphUtil}

import scala.collection.JavaConverters._

class ObjectReaderTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  implicit val config: PublishConfig = mock[PublishConfig](Mockito.withSettings().serializable())

  "Object Reader " should " read the metadata " in {
    when(mockJanusGraphUtil.getNodeProperties("do_123.img")).thenReturn(Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123.img", "IL_UNIQUE_ID" -> "do_123.img", "pkgVersion" -> 2.0.asInstanceOf[AnyRef]).asJava)
    val objectReader = new TestObjectReader()
    val readerConfig = ExtDataConfig("test", "test")
    val obj = objectReader.getObject("do_123", 2, "", "Public", readerConfig)
    val metadata = obj.metadata.asJava
    metadata.isEmpty should be(false)
    obj.extData should be(None)
    obj.hierarchy should be(None)
  }
}

class TestObjectReader extends ObjectReader {

  override def getExtData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): Option[ObjectExtData] = None

  override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): Option[Map[String, AnyRef]] = None

  override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None
}
