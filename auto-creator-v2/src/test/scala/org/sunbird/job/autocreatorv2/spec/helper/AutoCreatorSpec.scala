package org.sunbird.job.autocreatorv2.spec.helper

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.when
import org.neo4j.driver.v1.StatementResult
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.autocreatorv2.helpers.AutoCreator
import org.sunbird.job.autocreatorv2.model.ObjectData
import org.sunbird.job.autocreatorv2.util.CloudStorageUtil
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, Neo4JUtil, ScalaJsonUtil}

class AutoCreatorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

	implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
	implicit var cassandraUtil: CassandraUtil = _
	val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
	val jobConfig: AutoCreatorV2Config = new AutoCreatorV2Config(config)
	val defCache = new DefinitionCache()
	val qDefinition: ObjectDefinition = defCache.getDefinition("Question", jobConfig.schemaSupportVersionMap.getOrElse("question", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
	val qsDefinition: ObjectDefinition = defCache.getDefinition("QuestionSet", jobConfig.schemaSupportVersionMap.getOrElse("questionset", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
	implicit val cloudUtil : CloudStorageUtil = new CloudStorageUtil(jobConfig)
    var httpUtil: HttpUtil = mock[HttpUtil]

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
			case ex: Exception => {
			}
		}
	}

	def delay(time: Long): Unit = {
		try {
			Thread.sleep(time)
		} catch {
			case ex: Exception => print("")
		}
	}

	"getObject" should "return a valid object" in {
		val downloadUrl = "https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/added1_1616751462043_do_113244425048121344131_1_SPINE.ecar"
		val result = new TestAutoCreator().getObject("do_113244425048121344131", "QuestionSet", downloadUrl)(jobConfig, httpUtil, qsDefinition)
		result.identifier shouldEqual "do_113244425048121344131"
		result.metadata.nonEmpty shouldBe(true)
	}

	"enrichMetadata" should "return object with updated meta" in {
		val downloadUrl = "https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/added1_1616751462043_do_113244425048121344131_1_SPINE.ecar"
		val data = new ObjectData("do_123", "QuestionSet", Map("downloadUrl"->""), Some(Map()), Some(Map()))
		val eventMeta = Map("downloadUrl"->downloadUrl, "lastPublishedBy"->"testUser", "pdfUrl"->"testUrl", "previewUrl"->"testUrl", "variants"->Map("spine"->downloadUrl))
		val result = new TestAutoCreator().enrichMetadata(data, eventMeta)(jobConfig)
		result.metadata.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String] shouldEqual "do_123"
		result.metadata.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String] shouldEqual "QuestionSet"
		result.metadata.getOrElse("IL_SYS_NODE_TYPE", "").asInstanceOf[String] shouldEqual "DATA_NODE"
	}

	"processCloudMeta" should "return object with updated cloud urls" in {
		when(httpUtil.getSize(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(110)
		val downloadUrl = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/question/do_113264104174723072120/test-1_1619640554144_do_113264104174723072120_15.ecar"
		val data = new ObjectData("do_123", "QuestionSet", Map("downloadUrl" -> downloadUrl, "variants" -> Map("spine"->Map("ecarUrl" -> downloadUrl, "size"-> 1220), "online"->Map("ecarUrl" -> downloadUrl, "size"->2321))), Some(Map()), Some(Map()))
		val result = new TestAutoCreator().processCloudMeta(data)(jobConfig, cloudUtil, httpUtil)
		result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String].nonEmpty shouldBe(true)
	}

	"processChildren" should "create and return the children object" in {
		when(httpUtil.getSize(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(110)
		val resultMap = Map("result"-> Map("content"->Map("name"->"Test Data")))
		val response = HTTPResponse(200, ScalaJsonUtil.serialize(resultMap))
		when(httpUtil.get(ArgumentMatchers.anyString(),ArgumentMatchers.any[Map[String, String]]())).thenReturn(response)
		val mockResult = mock[StatementResult](Mockito.withSettings().serializable())
		when(mockNeo4JUtil.executeQuery(anyString())).thenReturn(mockResult)
		val child: Map[String, AnyRef] = Map("do_113264104174723072120" -> Map("objectType" -> "Question", "downloadUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/question/do_113264104174723072120/test-1_1619640554144_do_113264104174723072120_15.ecar", "variants"-> Map("full" -> Map("ecarUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/question/do_113264104174723072120/test-1_1619640554144_do_113264104174723072120_15.ecar", "size"->2321), "online"->Map("ecarUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/question/do_113264104174723072120/test-1_1619640555385_do_113264104174723072120_15_ONLINE.ecar", "size"-> 2322))))
		val result: Map[String, ObjectData] = new TestAutoCreator().processChildren(child)(jobConfig, mockNeo4JUtil, cassandraUtil, cloudUtil, defCache, httpUtil)
		result should not be empty
    result.keys should contain ("do_113264104174723072120")
		result.get("do_113264104174723072120").get.identifier shouldEqual "do_113264104174723072120"
	}


}

class TestAutoCreator extends AutoCreator {}
