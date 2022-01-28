package org.sunbird.job.contentautocreator.spec.helper

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, anyMap, anyString, contains, endsWith}
import org.mockito.Mockito.when
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.driver.v1.StatementResult
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.contentautocreator.domain.Event
import org.sunbird.job.contentautocreator.helpers.ContentAutoCreator
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.task.ContentAutoCreatorConfig
import org.sunbird.job.util._

class ContentAutoCreatorSpec extends FlatSpec with Matchers with MockitoSugar {

	implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
	val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
	val jobConfig: ContentAutoCreatorConfig = new ContentAutoCreatorConfig(config)
	val defCache = new DefinitionCache()
	implicit val cloudUtil : CloudStorageUtil = new CloudStorageUtil(jobConfig)
	var httpUtil: HttpUtil = mock[HttpUtil]

	val contentResponse = """{"responseCode":"OK","result":{"content":{"identifier":"do_21344892893869670417014","name":"Aparna","description":"about water","source":"https://drive.google.com/uc?export=download&id=1ZAW528VDqHNV6R3lTXfDOxhu9hyAXVl1","artifactUrl":"https://drive.google.com/uc?export=download&id=1ZAW528VDqHNV6R3lTXfDOxhu9hyAXVl1","appIcon":"https://drive.google.com/uc?export=download&id=1-tWar0Kl6DsuUpZ36f-3yF1_fDBhxzNj","creator":"Aparna","author":"Aparna", "versionKey":"1587624624051","audience":["Student"],"code":"1ee4c91a-61a4-2b5e-e3bd-a19c567242fc","mimeType":"application/pdf","primaryCategory":"eTextbook","lastPublishedBy":"9cb68c8f-7c23-476e-a5bf-11978f07e28b","createdBy":"9cb68c8f-7c23-476e-a5bf-11978f07e28b","programId":"a4597550-7119-11ec-902a-3b5d30502ba5","copyright":"2013","attributions":["Nadiya Anusha"],"keywords":["Drop"],"contentPolicyCheck":true,"channel":"01329314824202649627","framework":"ekstep_ncert_k-12","board":"CBSE","medium":["English"],"gradeLevel":["Class 1"],"subject":["English"],"boardIds":["ekstep_ncert_k-12_board_cbse"],"mediumIds":["ekstep_ncert_k-12_medium_english"],"gradeLevelIds":["ekstep_ncert_k-12_gradelevel_class1"],"subjectIds":["ekstep_ncert_k-12_subject_english"],"targetFWIds":["ekstep_ncert_k-12"],"license":"CC BY 4.0","processId":"761fd6b4-1478-4e0e-9c00-fe5aba11173c","objectType":"Content","status":"Draft"}}}"""
	val createResponse = """{"responseCode":"OK","result":{"identifier":"do_21344892893869670417014", "versionKey":"1587624624051"}}"""
	val uploadResponse = """{"responseCode":"OK","result":{"identifier":"do_21344892893869670417014", "artifactUrl": "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_21344892893869670417014/artifact/cbse-copy-143.pdf"}}}"""
	val reviewResponse = """{"responseCode":"OK","result":{"node_id":"do_21344892893869670417014"}}"""
	val publishResponse = """{"responseCode":"OK","result":{"publishStatus":"Success"}}"""

	def delay(time: Long): Unit = {
		try {
			Thread.sleep(time)
		} catch {
			case ex: Exception => print("")
		}
	}

	"process" should "not throw exception" in {
		val dockEvent = "{\"eid\":\"BE_JOB_REQUEST\",\"ets\":1641714958337,\"mid\":\"LP.1641714958337.30e5f007-b878-4406-91d1-2a77a72911cb\",\"actor\":{\"id\":\"Auto Creator\",\"type\":\"System\"},\"context\":{\"pdata\":{\"id\":\"org.sunbird.platform\",\"ver\":\"1.0\",\"env\":\"staging\"},\"channel\":\"01329314824202649627\"},\"object\":{\"id\":\"do_21344892893869670417014\",\"ver\":null},\"edata\":{\"action\":\"auto-create\",\"originData\":{},\"iteration\":1,\"metadata\":{\"name\":\"Aparna\",\"description\":\"about water\",\"source\":\"https://drive.google.com/uc?export=download&id=1ZAW528VDqHNV6R3lTXfDOxhu9hyAXVl1\",\"artifactUrl\":\"https://drive.google.com/uc?export=download&id=1ZAW528VDqHNV6R3lTXfDOxhu9hyAXVl1\",\"appIcon\":\"https://drive.google.com/uc?export=download&id=1-tWar0Kl6DsuUpZ36f-3yF1_fDBhxzNj\",\"creator\":\"Aparna\",\"author\":\"Aparna\",\"audience\":[\"Student\"],\"code\":\"1ee4c91a-61a4-2b5e-e3bd-a19c567242fc\",\"mimeType\":\"application/pdf\",\"primaryCategory\":\"eTextbook\",\"lastPublishedBy\":\"9cb68c8f-7c23-476e-a5bf-11978f07e28b\",\"createdBy\":\"9cb68c8f-7c23-476e-a5bf-11978f07e28b\",\"programId\":\"a4597550-7119-11ec-902a-3b5d30502ba5\",\"copyright\":\"2013\",\"attributions\":[\"Nadiya Anusha\"],\"keywords\":[\"Drop\"],\"contentPolicyCheck\":true,\"channel\":\"01329314824202649627\",\"framework\":\"ekstep_ncert_k-12\",\"board\":\"CBSE\",\"medium\":[\"English\"],\"gradeLevel\":[\"Class 1\"],\"subject\":[\"English\"],\"boardIds\":[\"ekstep_ncert_k-12_board_cbse\"],\"mediumIds\":[\"ekstep_ncert_k-12_medium_english\"],\"gradeLevelIds\":[\"ekstep_ncert_k-12_gradelevel_class1\"],\"subjectIds\":[\"ekstep_ncert_k-12_subject_english\"],\"targetFWIds\":[\"ekstep_ncert_k-12\"],\"collectionId\":\"do_21344890175651020816870\",\"unitIdentifiers\":[\"do_21344890175781273616871\"],\"license\":\"CC BY 4.0\",\"processId\":\"761fd6b4-1478-4e0e-9c00-fe5aba11173c\",\"objectType\":\"Content\"},\"identifier\":\"do_21344892893869670417014\",\"collection\":[{\"identifier\":\"do_21344890175651020816870\",\"unitId\":\"do_21344890175781273616871\"}],\"objectType\":\"Content\",\"stage\":\"publish\"}}"
		val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](dockEvent),0,1)
		when(httpUtil.post(contains("/v3/search"), anyString, any())).thenReturn(HTTPResponse(200, ScalaJsonUtil.serialize(Map.empty[String, AnyRef])))
		when(httpUtil.post(contains("/content/v4/create"), anyString, any())).thenReturn(HTTPResponse(200, createResponse))
		when(httpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, contentResponse))
		when(httpUtil.patch(contains("/content/v4/update"), anyString, any())).thenReturn(HTTPResponse(200, createResponse))
		when(httpUtil.postFilePath(contains("/content/v4/upload"), anyString, anyString, any())).thenReturn(HTTPResponse(200, uploadResponse))
		when(httpUtil.post(contains("/content/v4/review"), anyString, any())).thenReturn(HTTPResponse(200, reviewResponse))
		when(httpUtil.post(contains("/content/v3/publish"), anyString, any())).thenReturn(HTTPResponse(200, publishResponse))
		new TestContentAutoCreator().process(jobConfig, event, httpUtil, mockNeo4JUtil, cloudUtil)
	}


}

class TestContentAutoCreator extends ContentAutoCreator {}
