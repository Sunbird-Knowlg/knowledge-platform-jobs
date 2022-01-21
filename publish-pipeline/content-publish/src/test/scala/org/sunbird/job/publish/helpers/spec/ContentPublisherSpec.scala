package org.sunbird.job.publish.helpers.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.content.publish.helpers.ContentPublisher
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}

import scala.concurrent.ExecutionContextExecutor

class ContentPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: ContentPublishConfig = new ContentPublishConfig(config)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.contentKeyspaceName, jobConfig.contentTableName)
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = jobConfig.asInstanceOf[PublishConfig]
  implicit val httpUtil: HttpUtil = new HttpUtil

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
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/pdf"))
    val result: ObjectData = new TestContentPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
  }

  ignore should "enrich the Content metadata for application/vnd.ekstep.html-archive should through exception in artifactUrl is not available" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.html-archive"))
    val result: ObjectData = new TestContentPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
  }

  "enrichObjectMetadata" should "enrich the Content metadata for application/vnd.ekstep.html-archive" in {
    val data = new ObjectData("do_1132167819505500161297", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_1132167819505500161297", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.html-archive", "artifactUrl" -> "artifactUrl.zip"))
    val result: ObjectData = new TestContentPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
  }

  "validateMetadata with invalid external data" should "return exception messages" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("artifactUrl" -> "artifactUrl")))
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(1)
  }

  "validateMetadata with mimeType application/vnd.ekstep.ecml-archive " should " return exception messages if extData is set as None" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.ecml-archive"), None)
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(1)
    result.contains("Either 'body' or 'artifactUrl' are required for processing of ECML content for : do_123") shouldBe true
  }

  "validateMetadata with mimeType application/vnd.ekstep.ecml-archive " should " return exception messages if is having body=\"\"" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.ecml-archive"), Some(Map[String, AnyRef]("body" -> "")))
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(1)
    result.contains("Either 'body' or 'artifactUrl' are required for processing of ECML content for : do_123") shouldBe true
  }

  "validateMetadata with mimeType application/vnd.ekstep.ecml-archive " should " return exception messages if is having body=null" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.ecml-archive"), Some(Map[String, AnyRef]("body" -> null)))
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(1)
    result.contains("Either 'body' or 'artifactUrl' are required for processing of ECML content for : do_123") shouldBe true
  }

  "validateMetadata with mimeType application/vnd.ekstep.ecml-archive " should " not return exception messages if is having body=null but artifactUrl is available" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.ecml-archive", "artifactUrl" -> "sampleUrl"), Some(Map[String, AnyRef]("body" -> null)))
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(0)
  }

  "validateMetadata with mimeType application/vnd.ekstep.ecml-archive " should " not return exception messages if is having valid body" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.ecml-archive"), Some(Map[String, AnyRef]("body" -> Map("sampleKey" -> "sampleValue"))))
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(0)
  }

  "validateMetadata with mimeType video/x-youtube or video/youtube " should " return exception messages if content is having invalid artifactUrl" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "video/x-youtube", "artifactUrl" -> "https://www.youtube.com/"), None)
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(1)
    result.contains("Invalid youtube Url = https://www.youtube.com/ for : do_123") shouldBe true
  }

  "validateMetadata with mimeType video/x-youtube or video/youtube " should " not return exception messages if content is having valid artifactUrl = https://www.youtube.com/embed/watch?" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "video/x-youtube", "artifactUrl" -> "https://www.youtube.com/embed/watch?"), None)
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(0)
  }

  "validateMetadata with mimeType video/x-youtube or video/youtube " should " not return exception messages if content is having valid artifactUrl = https://www.youtube.com/watch?v=6Js8tBCfbWk" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "video/x-youtube", "artifactUrl" -> "https://www.youtube.com/watch?v=6Js8tBCfbWk"), None)
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(0)
  }

  "validateMetadata with mimeType video/x-youtube or video/youtube " should " not return exception messages if content is having valid artifactUrl = https://youtu.be/6Js8tBCfbWk" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "video/x-youtube", "artifactUrl" -> "https://youtu.be/6Js8tBCfbWk"), None)
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(0)
  }

  "validateMetadata with mimeType application/pdf " should " return exception messages if content is having invalid artifactUrl" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/pdf", "artifactUrl" -> "https://www.youtube.com/"), None)
    assertThrows[InvalidInputException] {
      val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
      result.size should be(1)
      result.contains("Error! Invalid File Extension. Uploaded file https://www.youtube.com/ is not a pdf file for : do_123") shouldBe true
    }
  }

  "validateMetadata with mimeType application/pdf " should " not return exception messages if content is having valid artifactUrl" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/pdf", "artifactUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11329603741667328018/artifact/do_11329603741667328018_1623058698775_intellijidea_referencecard.pdf"), None)
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(0)
  }

  "validateMetadata with mimeType application/epub " should " return exception messages if content is having invalid artifactUrl" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/epub", "artifactUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11329603741667328018/artifact/do_11329603741667328018_1623058698775_intellijidea_referencecard.pdf"), None)
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(1)
    result.contains("Error! Invalid File Extension. Uploaded file https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11329603741667328018/artifact/do_11329603741667328018_1623058698775_intellijidea_referencecard.pdf is not a epub file for : do_123") shouldBe true
  }

  "validateMetadata with mimeType application/msword " should " return exception messages if content is having invalid artifactUrl" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/msword", "artifactUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11329603741667328018/artifact/do_11329603741667328018_1623058698775_intellijidea_referencecard.pdf"), None)
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(1)
    result.contains("Error! Invalid File Extension. | Uploaded file https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_11329603741667328018/artifact/do_11329603741667328018_1623058698775_intellijidea_referencecard.pdf should be among the Allowed_file_extensions for mimeType doc [doc, docx, ppt, pptx, key, odp, pps, odt, wpd, wps, wks] for : do_123") shouldBe true
  }

  "saveExternalData " should "save external data to cassandra table" in {
    val data = new ObjectData("do_123", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    new TestContentPublisher().saveExternalData(data, readerConfig)
  }

  "getExtData " should " get content body for application/vnd.ekstep.ecml-archive mimeType " in {
    val identifier = "do_11321328578759884811663"
    val result: Option[ObjectExtData] = new TestContentPublisher().getExtData(identifier, 0.0, "application/vnd.ekstep.ecml-archive", readerConfig)
    result.getOrElse(new ObjectExtData).data.getOrElse(Map()).contains("body") shouldBe true
  }

  "getHierarchy " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestContentPublisher().getHierarchy(identifier, 1.0, readerConfig)
  }

  "getExtDatas " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestContentPublisher().getExtDatas(List(identifier), readerConfig)
  }

  "getHierarchies " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestContentPublisher().getHierarchies(List(identifier), readerConfig)
  }

  "getDataForEcar" should "return one element in list" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Content"), Some(Map("responseDeclaration" -> "test")), Some(Map()))
    val result: Option[List[Map[String, AnyRef]]] = new TestContentPublisher().getDataForEcar(data)
    result.size should be(1)
  }

  "getObjectWithEcar" should "return object with ecar url" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Content", "identifier" -> "do_123", "name" -> "Test PDF Content"), Some(Map("responseDeclaration" -> "test", "media" -> "[{\"id\":\"do_1127129497561497601326\",\"type\":\"image\",\"src\":\"/content/do_1127129497561497601326.img/artifact/sunbird_1551961194254.jpeg\",\"baseUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev\"}]")), Some(Map()))
    val result = new TestContentPublisher().getObjectWithEcar(data, List(EcarPackageType.FULL, EcarPackageType.ONLINE))(ec, cloudStorageUtil, jobConfig, defCache, defConfig, httpUtil)
    StringUtils.isNotBlank(result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String])
  }
}

class TestContentPublisher extends ContentPublisher {}
