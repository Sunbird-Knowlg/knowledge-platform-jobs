package org.sunbird.job.publish.helpers.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, when}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.content.publish.helpers.CollectionPublisher
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, ElasticSearchUtil, HttpUtil, Neo4JUtil, ScalaJsonUtil}

import scala.concurrent.ExecutionContextExecutor

class CollectionPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: ContentPublishConfig = new ContentPublishConfig(config)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.hierarchyKeyspaceName, jobConfig.hierarchyTableName)
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = jobConfig.asInstanceOf[PublishConfig]
  implicit val httpUtil: HttpUtil = new HttpUtil
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val mockElasticUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  var definitionCache = new DefinitionCache()
  implicit val definition: ObjectDefinition = definitionCache.getDefinition("Collection", jobConfig.schemaSupportVersionMap.getOrElse("collection", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
      delay(10000)
    } catch {
      case ex: Exception =>
    }
  }

  def delay(time: Long): Unit = {
    try {
      Thread.sleep(time)
    } catch {
      case ex: Exception => print("")
    }
  }

  "enrichObjectMetadata" should "enrich the Content pkgVersion metadata" in {
    val data = new ObjectData("do_2133950809948078081503", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_2133950809948078081503", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.content-collection"))
    val result: ObjectData = new TestCollectionPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
  }

  //  "validateMetadata with invalid external data" should "return exception messages" in {
  //    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("artifactUrl" -> "artifactUrl")))
  //    val result: List[String] = new TestCollectionPublisher().validateMetadata(data, data.identifier)
  //    result.size should be(1)
  //  }

  "saveExternalData " should "save external data to cassandra table" in {
    val data = new ObjectData("do_123", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    new TestCollectionPublisher().saveExternalData(data, readerConfig)
  }

  "getHierarchy " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestCollectionPublisher().getHierarchy(identifier, 1.0, readerConfig)
  }

  "getExtDatas " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestCollectionPublisher().getExtDatas(List(identifier), readerConfig)
  }

  "getHierarchies " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestCollectionPublisher().getHierarchies(List(identifier), readerConfig)
  }

  "getDataForEcar" should "return one element in list" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Content"), Some(Map("responseDeclaration" -> "test")), Some(Map()))
    val result: Option[List[Map[String, AnyRef]]] = new TestCollectionPublisher().getDataForEcar(data)
    result.size should be(1)
  }

  "getObjectWithEcar" should "return object with ecar url" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Collection", "identifier" -> "do_123", "name" -> "Test Collection"), Some(Map()), Some(Map()))
    val result = new TestCollectionPublisher().getObjectWithEcar(data, List(EcarPackageType.SPINE.toString, EcarPackageType.ONLINE.toString))(ec, mockNeo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, jobConfig, defCache, defConfig, httpUtil)
    StringUtils.isNotBlank(result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String])
  }

  "syncNodes" should "sync Child Nodes into ElasticSearch" in {
    Mockito.reset(mockElasticUtil)
    doNothing().when(mockElasticUtil).addDocument(anyString(), anyString())
    doNothing().when(mockElasticUtil).bulkIndexWithIndexId(anyString(), anyString(), any())

    val do_113405023736512512114Json = """{"identifier":"do_113405023736512512114","graph_id":"domain","node_id":0,"collections":["do_11340502373639782416", "do_113405023736479744112"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    when(mockElasticUtil.getDocumentAsString("do_113405023736512512114")).thenReturn(do_113405023736512512114Json)

    val do_11340502373639782416Json = """{"identifier":"do_11340502373639782416","graph_id":"domain","node_id":0,"collections":["do_11340502373642240018", "do_11340502373608652812"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    when(mockElasticUtil.getDocumentAsString("do_11340502373639782416")).thenReturn(do_11340502373639782416Json)

    val do_11340502373642240018Json = """{"identifier":"do_11340502373642240018","graph_id":"domain","node_id":0,"collections":["do_11336831941257625611"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    when(mockElasticUtil.getDocumentAsString("do_11340502373642240018")).thenReturn(do_11340502373642240018Json)

    val do_11340502373608652812Json = """{"identifier":"do_11340502373608652812","graph_id":"domain","node_id":0,"collections":["do_11340096165525094411"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    when(mockElasticUtil.getDocumentAsString("do_11340502373608652812")).thenReturn(do_11340502373608652812Json)

    val do_113405023736479744112Json = """{"identifier":"do_113405023736479744112","graph_id":"domain","node_id":0,"collections":["do_11340502373638144014"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    when(mockElasticUtil.getDocumentAsString("do_113405023736479744112")).thenReturn(do_113405023736479744112Json)

    val do_11340502373638144014Json = """{"identifier":"do_11340502373638144014","graph_id":"domain","node_id":0,"collections":["do_113405023736455168110"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    when(mockElasticUtil.getDocumentAsString("do_11340502373638144014")).thenReturn(do_11340502373638144014Json)

    val do_113405023736455168110Json = """{"identifier":"do_113405023736455168110","graph_id":"domain","node_id":0,"collections":["do_11340096293585715212"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    when(mockElasticUtil.getDocumentAsString("do_113405023736455168110")).thenReturn(do_113405023736455168110Json)

    val syncChildrenData = ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](syncChildrenTestData)
    val messages: Map[String, Map[String, AnyRef]] = new TestCollectionPublisher().syncNodes(syncChildrenData, List.empty)(mockElasticUtil, mockNeo4JUtil, cassandraUtil, readerConfig, definition, jobConfig)

    assert(messages != null && messages.size > 0)

  }



  val syncChildrenTestData = "[{\"lastStatusChangedOn\":\"2021-11-08T12:40:36.586+0530\",\"parent\":\"do_11340502356035174411\",\"children\":[{\"lastStatusChangedOn\":\"2021-11-08T12:40:36.572+0530\",\"parent\":\"do_113405023736512512114\",\"children\":[{\"lastStatusChangedOn\":\"2021-11-08T12:40:36.575+0530\",\"parent\":\"do_11340502373639782416\",\"children\":[{\"copyright\":\"J H S BHARKHOKHA, Tamil Nadu\",\"lastStatusChangedOn\":\"2021-09-17T16:22:50.404+0530\",\"parent\":\"do_11340502373642240018\",\"licenseterms\":\"By creating any type of content (resources, books, courses etc.) on DIKSHA, you consent to publish it under the Creative Commons License Framework. Please choose the applicable creative commons license you wish to apply to your content.\",\"organisation\":[\"J H S BHARKHOKHA\",\"Tamil Nadu\"],\"mediaType\":\"content\",\"name\":\"jaga Aug 25th more than 200mp mp4 update 1\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-09-17T16:05:29.019+0530\",\"channel\":\"0126825293972439041\",\"lastUpdatedOn\":\"2021-09-17T16:22:50.404+0530\",\"size\":363062652,\"identifier\":\"do_11336831941257625611\",\"resourceType\":\"Learn\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"eTextbook\",\"appIcon\":\"https://preprodall.blob.core.windows.net/ntp-content-preprod/content/do_2133520666218741761984/artifact/do_2132664483637248001189_1619439497677_1200px-flag_of_india.thumb.svg.thumb.png\",\"languageCode\":[\"en\"],\"downloadUrl\":\"\",\"framework\":\"tn_k-12_5\",\"creator\":\"सामग्री निर्माता TN\",\"versionKey\":\"1631875539805\",\"mimeType\":\"video/mp4\",\"code\":\"3bd7411e-c03c-4997-a247-4d43a5cc820b\",\"license\":\"CC BY 4.0\",\"version\":2,\"prevStatus\":\"Live\",\"contentType\":\"Resource\",\"prevState\":\"Draft\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-09-17T16:22:15.047+0530\",\"objectType\":\"Content\",\"status\":\"Live\",\"createdBy\":\"fca2925f-1eee-4654-9177-fece3fd6afc9\",\"dialcodeRequired\":\"No\",\"interceptionPoints\":{},\"idealScreenSize\":\"normal\",\"contentEncoding\":\"identity\",\"depth\":4,\"consumerId\":\"2eaff3db-cdd1-42e5-a611-bebbf906e6cf\",\"lastPublishedBy\":\"\",\"osId\":\"org.ekstep.quiz.app\",\"copyrightYear\":2021,\"se_FWIds\":[\"tn_k-12_5\"],\"contentDisposition\":\"online-only\",\"previewUrl\":\"https://preprodall.blob.core.windows.net/ntp-content-preprod/content/assets/do_2133520666218741761984/como-kids-tv-_-the-story-of-comos-family-_-30min-_-cartoon-video-for-kids.mp4\",\"artifactUrl\":\"https://preprodall.blob.core.windows.net/ntp-content-preprod/content/assets/do_2133520666218741761984/como-kids-tv-_-the-story-of-comos-family-_-30min-_-cartoon-video-for-kids.mp4\",\"visibility\":\"Default\",\"credentials\":{\"enabled\":\"No\"},\"variants\":{\"spine\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11336831941257625611/jaga-aug-25th-more-than-200mp-mp4-update-1_1631875935857_do_11336831941257625611_3_SPINE.ecar\",\"size\":\"2172\"}},\"index\":1,\"pkgVersion\":3,\"idealScreenDensity\":\"hdpi\"}],\"mediaType\":\"content\",\"name\":\"5.1.1 Key parts in the head\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-08T12:40:36.575+0530\",\"channel\":\"0126825293972439041\",\"generateDIALCodes\":\"No\",\"lastUpdatedOn\":\"2021-11-08T12:46:52.715+0530\",\"identifier\":\"do_11340502373642240018\",\"description\":\"xyz\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Textbook Unit\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\",\"framework\":\"ncert_k-12\",\"versionKey\":\"1636355436575\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"code\":\"7991af5c2e51e4d3d7b83167aaac8829\",\"license\":\"CC BY 4.0\",\"leafNodes\":[\"do_11336831941257625611\"],\"version\":2,\"contentType\":\"TextBookUnit\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-08T12:53:09.398+0530\",\"objectType\":\"Collection\",\"status\":\"Live\",\"dialcodeRequired\":\"No\",\"idealScreenSize\":\"normal\",\"contentEncoding\":\"gzip\",\"leafNodesCount\":1,\"depth\":3,\"osId\":\"org.ekstep.launcher\",\"contentDisposition\":\"inline\",\"visibility\":\"Parent\",\"credentials\":{\"enabled\":\"No\"},\"variants\":\"{\\\"spine\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\\\",\\\"size\\\":\\\"12044\\\"},\\\"online\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202412_do_11340502356035174411_1_ONLINE.ecar\\\",\\\"size\\\":\\\"5074\\\"}}\",\"index\":1,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"},{\"lastStatusChangedOn\":\"2021-11-08T12:40:36.534+0530\",\"parent\":\"do_11340502373639782416\",\"children\":[{\"lastStatusChangedOn\":\"2021-11-02T19:13:39.729+0530\",\"parent\":\"do_11340502373608652812\",\"mediaType\":\"content\",\"name\":\"Collection Publishing PDF Content\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-02T18:56:17.917+0530\",\"createdFor\":[\"01309282781705830427\"],\"channel\":\"0126825293972439041\",\"lastUpdatedOn\":\"2021-11-02T19:13:39.729+0530\",\"streamingUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/assets/do_1134009478823116801129/chapter_1.pdf\",\"identifier\":\"do_11340096165525094411\",\"resourceType\":\"Learn\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":4,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Explanation Content\",\"appIcon\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11340094790233292811/artifact/033019_sz_reviews_feat_1564126718632.thumb.jpg\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11340096165525094411/collection-publishing-pdf-content_1635860615969_do_11340096165525094411_1.ecar\",\"framework\":\"ekstep_ncert_k-12\",\"creator\":\"N131\",\"versionKey\":\"1635859577917\",\"mimeType\":\"application/pdf\",\"code\":\"c9ce1ce0-b9b4-402e-a9c3-556701070838\",\"license\":\"CC BY 4.0\",\"version\":2,\"prevStatus\":\"Processing\",\"contentType\":\"Resource\",\"prevState\":\"Draft\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-02T19:13:35.589+0530\",\"objectType\":\"Content\",\"status\":\"Live\",\"pragma\":[\"external\"],\"createdBy\":\"0b71985d-fcb0-4018-ab14-83f10c3b0426\",\"dialcodeRequired\":\"No\",\"interceptionPoints\":{},\"keywords\":[\"CPPDFContent1\",\"CPPDFContent2\",\"CollectionKW1\"],\"idealScreenSize\":\"normal\",\"contentEncoding\":\"identity\",\"depth\":4,\"lastPublishedBy\":\"\",\"osId\":\"org.ekstep.quiz.app\",\"copyrightYear\":2021,\"se_FWIds\":[\"ekstep_ncert_k-12\"],\"contentDisposition\":\"inline\",\"previewUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/assets/do_1134009478823116801129/chapter_1.pdf\",\"artifactUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/assets/do_1134009478823116801129/chapter_1.pdf\",\"visibility\":\"Default\",\"credentials\":{\"enabled\":\"No\"},\"variants\":{\"full\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11340096165525094411/collection-publishing-pdf-content_1635860615969_do_11340096165525094411_1.ecar\",\"size\":\"256918\"},\"spine\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11340096165525094411/collection-publishing-pdf-content_1635860619148_do_11340096165525094411_1_SPINE.ecar\",\"size\":\"6378\"}},\"index\":1,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"}],\"mediaType\":\"content\",\"name\":\"5.1.2 Other parts\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-08T12:40:36.534+0530\",\"channel\":\"0126825293972439041\",\"generateDIALCodes\":\"No\",\"lastUpdatedOn\":\"2021-11-08T12:46:52.715+0530\",\"identifier\":\"do_11340502373608652812\",\"description\":\"\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Textbook Unit\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\",\"framework\":\"ncert_k-12\",\"versionKey\":\"1636355436534\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"code\":\"3bf70f06d3e8dba010d8806fd94259b1\",\"license\":\"CC BY 4.0\",\"leafNodes\":[\"do_11340096165525094411\"],\"version\":2,\"contentType\":\"TextBookUnit\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-08T12:53:09.398+0530\",\"objectType\":\"Collection\",\"status\":\"Live\",\"dialcodeRequired\":\"No\",\"idealScreenSize\":\"normal\",\"contentEncoding\":\"gzip\",\"leafNodesCount\":1,\"depth\":3,\"osId\":\"org.ekstep.launcher\",\"contentDisposition\":\"inline\",\"visibility\":\"Parent\",\"credentials\":{\"enabled\":\"No\"},\"variants\":\"{\\\"spine\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\\\",\\\"size\\\":\\\"12044\\\"},\\\"online\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202412_do_11340502356035174411_1_ONLINE.ecar\\\",\\\"size\\\":\\\"5074\\\"}}\",\"index\":2,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"}],\"mediaType\":\"content\",\"name\":\"5.1 Parts of Body\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-08T12:40:36.572+0530\",\"channel\":\"0126825293972439041\",\"generateDIALCodes\":\"No\",\"lastUpdatedOn\":\"2021-11-08T12:46:52.715+0530\",\"identifier\":\"do_11340502373639782416\",\"description\":\"This section describes about various part of the body such as head, hands, legs etc.\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Textbook Unit\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\",\"framework\":\"ncert_k-12\",\"versionKey\":\"1636355436572\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"code\":\"20cc1f31e62f924c6e47bf04c994376b\",\"license\":\"CC BY 4.0\",\"leafNodes\":[\"do_11336831941257625611\",\"do_11340096165525094411\"],\"version\":2,\"contentType\":\"TextBookUnit\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-08T12:53:09.398+0530\",\"objectType\":\"Collection\",\"status\":\"Live\",\"dialcodeRequired\":\"No\",\"idealScreenSize\":\"normal\",\"contentEncoding\":\"gzip\",\"leafNodesCount\":2,\"depth\":2,\"osId\":\"org.ekstep.launcher\",\"contentDisposition\":\"inline\",\"visibility\":\"Parent\",\"credentials\":{\"enabled\":\"No\"},\"variants\":\"{\\\"spine\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\\\",\\\"size\\\":\\\"12044\\\"},\\\"online\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202412_do_11340502356035174411_1_ONLINE.ecar\\\",\\\"size\\\":\\\"5074\\\"}}\",\"index\":1,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"},{\"lastStatusChangedOn\":\"2021-11-08T12:40:36.582+0530\",\"parent\":\"do_113405023736512512114\",\"children\":[{\"lastStatusChangedOn\":\"2021-11-08T12:40:36.570+0530\",\"parent\":\"do_113405023736479744112\",\"children\":[{\"lastStatusChangedOn\":\"2021-11-08T12:40:36.579+0530\",\"parent\":\"do_11340502373638144014\",\"children\":[{\"lastStatusChangedOn\":\"2021-11-02T19:16:10.667+0530\",\"parent\":\"do_113405023736455168110\",\"mediaType\":\"content\",\"name\":\"Collection Publish MP4 content\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-02T18:58:53.445+0530\",\"channel\":\"0126825293972439041\",\"lastUpdatedOn\":\"2021-11-02T19:16:10.667+0530\",\"identifier\":\"do_11340096293585715212\",\"resourceType\":\"Learn\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Explanation Content\",\"appIcon\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1134009488766730241130/artifact/033019_sz_reviews_feat_1564126718632.thumb.jpg\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11340096293585715212/collection-publish-mp4-content_1635860769119_do_11340096293585715212_1.ecar\",\"framework\":\"ekstep_ncert_k-12\",\"versionKey\":\"1635859733445\",\"mimeType\":\"video/mp4\",\"code\":\"e0b58864-3dc5-484a-b194-38c3eddcbce1\",\"license\":\"CC BY 4.0\",\"version\":2,\"prevStatus\":\"Draft\",\"contentType\":\"Resource\",\"prevState\":\"Draft\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-02T19:16:08.789+0530\",\"objectType\":\"Content\",\"status\":\"Live\",\"createdBy\":\"0b71985d-fcb0-4018-ab14-83f10c3b0426\",\"dialcodeRequired\":\"No\",\"interceptionPoints\":{},\"keywords\":[\"CPMP4ContentKW1\",\"CPMP4ContentKW2\"],\"idealScreenSize\":\"normal\",\"contentEncoding\":\"identity\",\"depth\":5,\"lastPublishedBy\":\"\",\"osId\":\"org.ekstep.quiz.app\",\"se_FWIds\":[\"ekstep_ncert_k-12\"],\"contentDisposition\":\"inline\",\"previewUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/assets/do_1134009488766730241130/amoeba-eat.mp4\",\"artifactUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/assets/do_1134009488766730241130/amoeba-eat.mp4\",\"visibility\":\"Default\",\"credentials\":{\"enabled\":\"No\"},\"variants\":{\"full\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11340096293585715212/collection-publish-mp4-content_1635860769119_do_11340096293585715212_1.ecar\",\"size\":\"2692101\"},\"spine\":{\"ecarUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11340096293585715212/collection-publish-mp4-content_1635860770277_do_11340096293585715212_1_SPINE.ecar\",\"size\":\"6275\"}},\"index\":1,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"}],\"mediaType\":\"content\",\"name\":\"dsffgdg\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-08T12:40:36.579+0530\",\"channel\":\"0126825293972439041\",\"generateDIALCodes\":\"No\",\"lastUpdatedOn\":\"2021-11-08T12:46:52.715+0530\",\"identifier\":\"do_113405023736455168110\",\"description\":\"\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Textbook Unit\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\",\"framework\":\"ncert_k-12\",\"versionKey\":\"1636355436579\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"code\":\"9cf84ff2fb08f9af4c23eb09df9b2520\",\"license\":\"CC BY 4.0\",\"leafNodes\":[\"do_11340096293585715212\"],\"version\":2,\"contentType\":\"TextBookUnit\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-08T12:53:09.398+0530\",\"objectType\":\"Collection\",\"status\":\"Live\",\"dialcodeRequired\":\"No\",\"idealScreenSize\":\"normal\",\"contentEncoding\":\"gzip\",\"leafNodesCount\":1,\"depth\":4,\"osId\":\"org.ekstep.launcher\",\"contentDisposition\":\"inline\",\"visibility\":\"Parent\",\"credentials\":{\"enabled\":\"No\"},\"variants\":\"{\\\"spine\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\\\",\\\"size\\\":\\\"12044\\\"},\\\"online\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202412_do_11340502356035174411_1_ONLINE.ecar\\\",\\\"size\\\":\\\"5074\\\"}}\",\"index\":1,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"}],\"mediaType\":\"content\",\"name\":\"5.2.1 Respiratory System\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-08T12:40:36.570+0530\",\"channel\":\"0126825293972439041\",\"generateDIALCodes\":\"No\",\"lastUpdatedOn\":\"2021-11-08T12:46:52.715+0530\",\"identifier\":\"do_11340502373638144014\",\"description\":\"\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Textbook Unit\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\",\"attributions\":[],\"framework\":\"ncert_k-12\",\"versionKey\":\"1636355436570\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"code\":\"b186b1bbcc9c58db865f75e34345179e\",\"license\":\"CC BY 4.0\",\"leafNodes\":[\"do_11340096293585715212\"],\"version\":2,\"contentType\":\"TextBookUnit\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-08T12:53:09.398+0530\",\"objectType\":\"Collection\",\"status\":\"Live\",\"dialcodeRequired\":\"No\",\"keywords\":[\"UnitKW1\",\"UnitKW2\"],\"idealScreenSize\":\"normal\",\"contentEncoding\":\"gzip\",\"leafNodesCount\":1,\"depth\":3,\"osId\":\"org.ekstep.launcher\",\"contentDisposition\":\"inline\",\"visibility\":\"Parent\",\"credentials\":{\"enabled\":\"No\"},\"variants\":\"{\\\"spine\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\\\",\\\"size\\\":\\\"12044\\\"},\\\"online\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202412_do_11340502356035174411_1_ONLINE.ecar\\\",\\\"size\\\":\\\"5074\\\"}}\",\"index\":1,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"}],\"mediaType\":\"content\",\"name\":\"5.2 Organ Systems\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-08T12:40:36.582+0530\",\"channel\":\"0126825293972439041\",\"generateDIALCodes\":\"No\",\"lastUpdatedOn\":\"2021-11-08T12:46:52.715+0530\",\"identifier\":\"do_113405023736479744112\",\"description\":\"\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Textbook Unit\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\",\"framework\":\"ncert_k-12\",\"versionKey\":\"1636355436582\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"code\":\"40a1ed37e0fad94eca76b2a96fe086ab\",\"license\":\"CC BY 4.0\",\"leafNodes\":[\"do_11340096293585715212\"],\"version\":2,\"contentType\":\"TextBookUnit\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-08T12:53:09.398+0530\",\"objectType\":\"Collection\",\"status\":\"Live\",\"dialcodeRequired\":\"No\",\"idealScreenSize\":\"normal\",\"contentEncoding\":\"gzip\",\"leafNodesCount\":1,\"depth\":2,\"osId\":\"org.ekstep.launcher\",\"contentDisposition\":\"inline\",\"visibility\":\"Parent\",\"credentials\":{\"enabled\":\"No\"},\"variants\":\"{\\\"spine\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\\\",\\\"size\\\":\\\"12044\\\"},\\\"online\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202412_do_11340502356035174411_1_ONLINE.ecar\\\",\\\"size\\\":\\\"5074\\\"}}\",\"index\":2,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"}],\"mediaType\":\"content\",\"name\":\"5. Human Body\",\"discussionForum\":{\"enabled\":\"No\"},\"createdOn\":\"2021-11-08T12:40:36.586+0530\",\"channel\":\"0126825293972439041\",\"generateDIALCodes\":\"No\",\"lastUpdatedOn\":\"2021-11-08T12:46:52.715+0530\",\"identifier\":\"do_113405023736512512114\",\"description\":\"This chapter describes about human body\",\"ownershipType\":[\"createdBy\"],\"compatibilityLevel\":1,\"audience\":[\"Student\"],\"os\":[\"All\"],\"primaryCategory\":\"Textbook Unit\",\"languageCode\":[\"en\"],\"downloadUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\",\"framework\":\"ncert_k-12\",\"versionKey\":\"1636355436586\",\"mimeType\":\"application/vnd.ekstep.content-collection\",\"code\":\"76abafa2a0c2cfef90b52db1ef41fb82\",\"license\":\"CC BY 4.0\",\"leafNodes\":[\"do_11340096293585715212\",\"do_11336831941257625611\",\"do_11340096165525094411\"],\"version\":2,\"contentType\":\"TextBookUnit\",\"language\":[\"English\"],\"lastPublishedOn\":\"2021-11-08T12:53:09.398+0530\",\"objectType\":\"Collection\",\"status\":\"Live\",\"dialcodeRequired\":\"No\",\"idealScreenSize\":\"normal\",\"contentEncoding\":\"gzip\",\"leafNodesCount\":3,\"depth\":1,\"osId\":\"org.ekstep.launcher\",\"contentDisposition\":\"inline\",\"visibility\":\"Parent\",\"credentials\":{\"enabled\":\"No\"},\"variants\":\"{\\\"spine\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202227_do_11340502356035174411_1_SPINE.ecar\\\",\\\"size\\\":\\\"12044\\\"},\\\"online\\\":{\\\"ecarUrl\\\":\\\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/collection/do_11340502356035174411/collection-publish-t20_1636356202412_do_11340502356035174411_1_ONLINE.ecar\\\",\\\"size\\\":\\\"5074\\\"}}\",\"index\":1,\"pkgVersion\":1,\"idealScreenDensity\":\"hdpi\"}]"



}





class TestCollectionPublisher extends CollectionPublisher {}
