package org.sunbird.job.spec.helper

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.helpers.AutoCreator
import org.sunbird.job.model.{ExtDataConfig, ObjectData}
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

class AutoCreatorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

	implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
	implicit var cassandraUtil: CassandraUtil = _
	val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
	val jobConfig: AutoCreatorV2Config = new AutoCreatorV2Config(config)
	val defCache = new DefinitionCache()
	val qDefinition: ObjectDefinition = defCache.getDefinition("Question", jobConfig.schemaSupportVersionMap.getOrElse("question", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
	val qsDefinition: ObjectDefinition = defCache.getDefinition("QuestionSet", jobConfig.schemaSupportVersionMap.getOrElse("questionset", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
	//val qReaderConfig: ExtDataConfig = ExtDataConfig(jobConfig., jobConfig.questionTableName)

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
		val result = new TestAutoCreator().getObject("do_113244425048121344131", "QuestionSet", downloadUrl, qsDefinition)(jobConfig)
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
		result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String] shouldEqual downloadUrl
	}

}

class TestAutoCreator extends AutoCreator {}
