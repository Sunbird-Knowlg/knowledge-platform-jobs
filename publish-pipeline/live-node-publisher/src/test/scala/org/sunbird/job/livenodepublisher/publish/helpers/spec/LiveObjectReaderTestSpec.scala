package org.sunbird.job.livenodepublisher.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.livenodepublisher.publish.helpers.LiveObjectReader
import org.sunbird.job.livenodepublisher.task.LiveNodePublisherConfig
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{ExtDataConfig, ObjectExtData}
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

import scala.collection.JavaConverters._

class LiveObjectReaderTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: LiveNodePublisherConfig = new LiveNodePublisherConfig(config)

  "Object Reader " should " read the metadata " in {
    when(mockNeo4JUtil.getNodeProperties("do_123.img")).thenReturn(Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123.img", "IL_UNIQUE_ID" -> "do_123.img", "pkgVersion" -> 2.0.asInstanceOf[AnyRef]).asJava)
    val objectReader = new TestObjectReader()
    val readerConfig = ExtDataConfig("test", "test")
    val obj = objectReader.getObject("do_123", 2, "", "Public", readerConfig)
    val metadata = obj.metadata.asJava
    metadata.isEmpty should be(false)
    obj.extData should be(None)
    obj.hierarchy should be(None)
  }
}

class TestObjectReader extends LiveObjectReader {


  override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getExtData(identifier: String, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, jobConfig: PublishConfig): Option[ObjectExtData] = None

  override def getHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None
}
