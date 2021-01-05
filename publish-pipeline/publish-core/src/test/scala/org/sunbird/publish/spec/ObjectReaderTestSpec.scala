package org.sunbird.publish.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.helpers.ObjectReader
import org.mockito.Mockito
import org.mockito.Mockito._

import scala.collection.JavaConverters._

class ObjectReaderTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())

  "Object Reader " should " read the metadata " in {
    when(mockNeo4JUtil.getNodeProperties("do_123")).thenReturn(Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123.img", "pkgVersion" -> 2.0.asInstanceOf[AnyRef]).asJava)
    val objectReader = new TestObjectReader()
    val obj = objectReader.getObject("do_123", 2)
    val metadata = obj.metadata.asJava
    metadata.isEmpty should be(false)
    obj.extData should be(None)
    obj.hierarchy should be(None)
  }
}

class TestObjectReader extends ObjectReader {

  override def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None
}
