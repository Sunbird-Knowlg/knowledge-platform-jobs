package org.sunbird.job.livenodepublisher.publish.helpers.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, when}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.livenodepublisher.publish.helpers.LiveCollectionPublisher
import org.sunbird.job.livenodepublisher.task.LiveNodePublisherConfig
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util._
import com.datastax.driver.core.Row

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.concurrent.ExecutionContextExecutor

import org.scalatest.Ignore

@Ignore
class LiveCollectionPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: LiveNodePublisherConfig = new LiveNodePublisherConfig(config)
  implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())
  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = jobConfig.asInstanceOf[PublishConfig]
  implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val mockElasticUtil: ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  var definitionCache = new DefinitionCache()
  implicit val definition: ObjectDefinition = definitionCache.getDefinition("Collection", jobConfig.schemaSupportVersionMap.getOrElse("collection", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.hierarchyKeyspaceName, jobConfig.hierarchyTableName, definition.getExternalPrimaryKey, definition.getExternalProps)

  def getTimeStamp: String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    sdf.format(new Date())
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "enrichObjectMetadata" should "enrich the Content pkgVersion metadata" in {
    val data = new ObjectData("do_2133950809948078081503", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_2133950809948078081503", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.content-collection", "keywords" -> Array[String]("test keyword")))
    val result: ObjectData = new TestCollectionPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue() should be(1)
  }

  "saveExternalData " should "save external data to cassandra table" in {
    val data = new ObjectData("do_1234", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    new TestCollectionPublisher().saveExternalData(data, readerConfig)
  }

  "getRelationalMetadata" should "return empty Map when there is no entry in relational_metadata column" in {
    when(cassandraUtil.findOne(anyString())).thenReturn(null)
    val collRelationalMetadata = new TestCollectionPublisher().getRelationalMetadata("do_1234", readerConfig)(cassandraUtil).get
    assert(collRelationalMetadata != null && collRelationalMetadata.isEmpty)
  }

  "publishHierarchy " should "save hierarchy data to cassandra table" in {
    val publishChildrenData = ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](publishedChildrenDataStr)
    val publishedCollectionNodeMetadataObj: Map[String,AnyRef] = ScalaJsonUtil.deserialize[Map[String,AnyRef]](publishedCollectionNodeMetadata)
    val data = new ObjectData("do_123", publishedCollectionNodeMetadataObj, Some(Map.empty[String, AnyRef]))
    val result = new TestCollectionPublisher().publishHierarchy(publishChildrenData, data, readerConfig, jobConfig)
    assert(result)
  }

  "getUnitsFromLiveContent" should "return object hierarchy" in {
    val mockRow = mock[Row]
    when(mockRow.getString("hierarchy")).thenReturn("{\"identifier\":\"do_123\", \"children\":[]}")
    when(cassandraUtil.findOne(anyString())).thenReturn(mockRow)
    
    val data = new ObjectData("do_2133950809948078081503", Map("identifier" -> "do_2133950809948078081503"), Some(Map.empty[String, AnyRef]))
    val fetchedChildren = new TestCollectionPublisher().getUnitsFromLiveContent(data)(cassandraUtil,readerConfig,jobConfig)
    assert(fetchedChildren.nonEmpty)
  }

  "updateOriginPkgVersion" should "return origin node Data" in {
    val metaData = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_11300581751853056018")
        put("identifier", "do_11300581751853056018")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"Yes\",\"autoBatch\":\"Yes\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
        put("pkgVersion", 3.asInstanceOf[AnyRef])
      }
    }
    val collectionObj = new ObjectData("do_123", Map("objectType" -> "Collection", "identifier" -> "do_123", "name" -> "Test Collection","origin" -> "do_456", "originData" -> Map("name" -> "Contemporary India  I", "copyType" -> "deep", "license" -> "CC BY 4.0", "organisation" -> Array("NCERT"))), Some(Map()), Some(Map()))
    when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(metaData)
    val originObj = new TestCollectionPublisher().updateOriginPkgVersion(collectionObj)
    assert(originObj.metadata("originData").asInstanceOf[Map[String,AnyRef]]("pkgVersion") == 3)
  }

  val publishedChildrenDataStr = "[{\"identifier\":\"do_123\", \"objectType\":\"Content\"}]"
  val publishedCollectionNodeMetadata = "{\"copyright\":\"tn\",\"lastStatusChangedOn\":\"2021-11-08T15:38:31.391+0530\",\"publish_type\":\"public\",\"author\":\"ContentcreatorTN\",\"identifier\":\"do_11340511118032076811\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"status\":\"Live\"}"
}

class TestCollectionPublisher extends LiveCollectionPublisher {}
