package org.sunbird.job.publish.helpers.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, when, doAnswer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.knowlg.publish.helpers.CollectionPublisher
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util._
import com.datastax.driver.core.Row
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.io.File
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContextExecutor

class CollectionPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())
  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = jobConfig.asInstanceOf[PublishConfig]
  implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val mockElasticUtil: ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  var definitionCache = new DefinitionCache()
  implicit val definition: ObjectDefinition = definitionCache.getDefinition("Collection", jobConfig.schemaSupportVersionMap.getOrElse("collection", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
  
  // Use correct hierarchy reader config for getLiveHierarchy
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.hierarchyKeyspaceName, jobConfig.hierarchyTableName, List("identifier"), definition.getExternalProps)

  def getTimeStamp: String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    sdf.format(new Date())
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val mockRow = mock[Row]
    when(mockRow.getString("identifier")).thenReturn("do_123")
    when(mockRow.getString("hierarchy")).thenReturn("""{"identifier":"do_123","children":[]}""")
    when(mockRow.getString("relational_metadata")).thenReturn("{}")
    
    val rowWithId = mock[Row]
    when(rowWithId.getString("identifier")).thenReturn("do_123")
    when(rowWithId.getString("hierarchy")).thenReturn("""{"identifier":"do_123","children":[]}""")
    when(rowWithId.getString("relational_metadata")).thenReturn("{}")

    val rowLive = mock[Row]
    when(rowLive.getString("identifier")).thenReturn("do_2133950809948078081503")
    when(rowLive.getString("hierarchy")).thenReturn("""{"identifier":"do_2133950809948078081503","children":[{"identifier":"do_1","visibility":"Parent","objectType":"Content","children":[]}]}""")

    when(cassandraUtil.findOne(anyString())).thenAnswer(new Answer[Row] {
      override def answer(invocation: InvocationOnMock): Row = {
        val query = invocation.getArgument[String](0)
        if (query.contains("do_2133950809948078081503")) rowLive
        else if (query.contains("do_123")) rowWithId
        else mockRow
      }
    })
    when(cassandraUtil.find(anyString())).thenAnswer(new Answer[java.util.List[Row]] {
      override def answer(invocation: InvocationOnMock): java.util.List[Row] = {
        val query = invocation.getArgument[String](0)
        if (query.contains("do_2133950809948078081503")) java.util.Arrays.asList(rowLive)
        else java.util.Arrays.asList(rowWithId)
      }
    })
    when(cassandraUtil.upsert(anyString())).thenReturn(true)
    when(cassandraUtil.executePreparedStatement(anyString(), any())).thenAnswer(new Answer[java.util.List[Row]] {
      override def answer(invocation: InvocationOnMock): java.util.List[Row] = java.util.Arrays.asList(mockRow)
    })
    
    when(cloudStorageUtil.uploadFile(any(), any(), any(), any())).thenReturn(Array("test-key", "test-url"))
    when(httpUtil.getSize(any(), any())).thenReturn(100)
    doAnswer(new Answer[File] {
      override def answer(invocation: InvocationOnMock): File = {
        val downloadPath = invocation.getArgument[String](1)
        val file = new File(downloadPath)
        file.getParentFile.mkdirs()
        if (!file.exists()) file.createNewFile()
        file
      }
    }).when(httpUtil).downloadFile(anyString(), anyString())
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

  "getDataForEcar" should "return one element in list" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Content"), Some(Map("responseDeclaration" -> "test")), Some(Map()))
    val result: Option[List[Map[String, AnyRef]]] = new TestCollectionPublisher().getDataForEcar(data)
    result.size should be(1)
  }

  "getObjectWithEcar" should "return object with ecar url" in {
    val unpublishedChildrenObj: List[Map[String, AnyRef]] = ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](unpublishedChildrenData)
    val data = new ObjectData("do_123", Map("objectType" -> "Collection", "identifier" -> "do_123", "name" -> "Test Collection", "lastPublishedOn" -> getTimeStamp, "lastUpdatedOn" -> getTimeStamp, "status" -> "Draft", "downloadUrl" -> "downloadUrl", "variants" -> Map.empty[String,AnyRef]), Some(Map()), Some(Map("children" -> unpublishedChildrenObj)))
    val result = new TestCollectionPublisher().getObjectWithEcar(data, List(EcarPackageType.SPINE, EcarPackageType.ONLINE))(ec, mockJanusGraphUtil, cassandraUtil, readerConfig, cloudStorageUtil, jobConfig, defCache, defConfig, httpUtil)
    StringUtils.isNotBlank(result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String]) should be (true)
  }

  "syncNodes" should "sync Child Nodes into ElasticSearch" in {
    Mockito.reset(mockElasticUtil)
    doNothing().when(mockElasticUtil).addDocument(anyString(), anyString())
    doNothing().when(mockElasticUtil).bulkIndexWithIndexId(anyString(), anyString(), any())

    val do_113405023736512512114Json = """{"identifier":"do_113405023736512512114","graph_id":"domain","node_id":0,"collections":["do_11340502373639782416", "do_113405023736479744112"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    when(mockElasticUtil.getDocumentAsString(anyString())).thenReturn(do_113405023736512512114Json)

    val publishedCollectionNodeMetadataObj: Map[String,AnyRef] = ScalaJsonUtil.deserialize[Map[String,AnyRef]](publishedCollectionNodeMetadata)
    val data = new ObjectData("do_123", publishedCollectionNodeMetadataObj, Some(Map.empty[String, AnyRef]))
    val syncChildrenData = ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](publishedChildrenData)
    val messages: Map[String, Map[String, AnyRef]] = new TestCollectionPublisher().syncNodes(data, syncChildrenData, List.empty)(mockElasticUtil, mockJanusGraphUtil, cassandraUtil, readerConfig, definition, jobConfig)

    assert(messages != null)
  }

  "updateHierarchyMetadata" should "update child nodes with published object metadata" in {
    val unpublishedChildrenObj: List[Map[String, AnyRef]] = ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](unpublishedChildrenData)
    val publishedCollectionNodeMetadataObj: Map[String,AnyRef] = ScalaJsonUtil.deserialize[Map[String,AnyRef]](publishedCollectionNodeMetadata)

    val collRelationalMetadata = Map.empty[String, AnyRef]
    val updatedChildren: List[Map[String, AnyRef]] = new TestCollectionPublisher().updateHierarchyMetadata(unpublishedChildrenObj, publishedCollectionNodeMetadataObj, collRelationalMetadata)(jobConfig)

    assert(updatedChildren.nonEmpty)
  }

  "getRelationalMetadata" should "return empty Map when there is no entry in relational_metadata column" in {
    val collRelationalMetadata = new TestCollectionPublisher().getRelationalMetadata("do_1234", 1, readerConfig)(cassandraUtil, jobConfig).get
    assert(collRelationalMetadata != null && collRelationalMetadata.isEmpty)
  }

  "publishHierarchy " should "save hierarchy data to cassandra table" in {
    val publishChildrenData = ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](publishedChildrenData)
    val publishedCollectionNodeMetadataObj: Map[String,AnyRef] = ScalaJsonUtil.deserialize[Map[String,AnyRef]](publishedCollectionNodeMetadata)
    val data = new ObjectData("do_123", publishedCollectionNodeMetadataObj, Some(Map.empty[String, AnyRef]))
    val result = new TestCollectionPublisher().publishHierarchy(publishChildrenData, data, readerConfig, jobConfig)
    assert(result)
  }

  "getUnitsFromLiveContent" should "return object hierarchy" in {
    val data = new ObjectData("do_2133950809948078081503", Map("identifier" -> "do_2133950809948078081503"), Some(Map.empty[String, AnyRef]))
    val fetchedChildren = new TestCollectionPublisher().getUnitsFromLiveContent(data)(cassandraUtil, readerConfig, jobConfig)
    assert(fetchedChildren.nonEmpty)
  }

  "updateOriginPkgVersion" should "return origin node Data" in {
    val metaData = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_11300581751853056018")
        put("identifier", "do_11300581751853056018")
        put("name", "Origin Content")
        put("pkgVersion", 3.asInstanceOf[AnyRef])
      }
    }
    val collectionObj = new ObjectData("do_123", Map("objectType" -> "Collection", "identifier" -> "do_123", "name" -> "Test Collection","origin" -> "do_456", "originData" -> Map("name" -> "Contemporary India  I", "pkgVersion" -> 1.asInstanceOf[AnyRef])), Some(Map()), Some(Map()))
    when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(metaData)
    val originObj = new TestCollectionPublisher().updateOriginPkgVersion(collectionObj)
    assert(originObj.metadata("originData").asInstanceOf[Map[String,AnyRef]]("pkgVersion") == 3)
  }

  val publishedChildrenData = "[{\"lastStatusChangedOn\":\"2021-11-08T12:40:36.586+0530\",\"parent\":\"do_11340502356035174411\",\"children\":[]}]"
  val unpublishedChildrenData = "[{\"lastStatusChangedOn\":\"2021-11-08T15:38:54.180+0530\",\"parent\":\"do_11340511118032076811\",\"children\":[], \"visibility\":\"Default\"}]"
  val publishedCollectionNodeMetadata = "{\"copyright\":\"tn\",\"lastStatusChangedOn\":\"2021-11-08T15:38:31.391+0530\",\"publish_type\":\"public\",\"author\":\"ContentcreatorTN\",\"children\":[],\"body\":null,\"mediaType\":\"content\",\"name\":\"Collection Publish T20\",\"toc_url\":\"https://test-container.blob.core.windows.net/content/do_11340511118032076811/artifact/do_11340511118032076811_toc.json\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-08T15:38:31.391+0530\",\"createdFor\":[\"0125196274181898243\"],\"channel\":\"0126825293972439041\",\"generateDIALCodes\":\"No\",\"lastUpdatedOn\":\"2021-11-08T15:53:33.587+0530\",\"size\":12048,\"publishError\":null,\"identifier\":\"do_11340511118032076811\",\"description\":\"Collection Publish\",\"resourceType\":\"Book\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"trackable\":{\"enabled\":\"No\",\"autoBatch\":\"No\"},\"os\":[\"All\"],\"primaryCategory\":\"Digital Textbook\",\"appIcon\":\"file:///tmp/test.pdf\",\"downloadUrl\":\"file:///tmp/test.pdf\",\"attributions\":[],\"framework\":\"ncert_k-12\",\"posterImage\":\"file:///tmp/test.pdf\",\"creator\":\"NCERT\",\"totalCompressedSize\":363062652,\"versionKey\":\"1636367013587\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"code\":\"0125196274181898243\",\"license\":\"CC BY 4.0\"}"

  "processChild" should "handle stringified JSON arrays for tagging properties" in {
    val childMetadata = Map("language" -> "[\"English\"]", "identifier" -> "do_123", "objectType" -> "Content")
    val result = new TestCollectionPublisher().processChild(childMetadata)
    result("language").asInstanceOf[Set[String]] should contain("English")
  }

  "processChild" should "handle plain strings for tagging properties" in {
    val childMetadata = Map("language" -> "English", "identifier" -> "do_123", "objectType" -> "Content")
    val result = new TestCollectionPublisher().processChild(childMetadata)
    result("language").asInstanceOf[Set[String]] should contain("English")
  }

}
class TestCollectionPublisher extends CollectionPublisher {}
