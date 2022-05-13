package org.sunbird.job.dialcodecontextupdater.spec.helper

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.ArgumentMatchers.{any, anyString, contains, endsWith}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.helpers.DialcodeContextUpdater
import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.util._


class DialcodeContextUpdaterSpec extends FlatSpec with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: DialcodeContextUpdaterConfig = new DialcodeContextUpdaterConfig(config)
  val defCache = new DefinitionCache()
  var mockHttpUtil: HttpUtil = mock[HttpUtil]

  "getContextData" should "return context Information" in {
    val contextResponse = """{"@context": {"schema": "http://schema.org/","identifier": {"@id": "schema:name#identifier","@type": "schema:name"},"channel": {"@id": "schema:name#channel","@type": "schema:name"},"publisher": {"@id": "schema:name#publisher","@type": "schema:name"},"batchCode": {"@id": "schema:name#batchCode","@type": "schema:name"},"status": {"@id": "schema:name#status","@type": "schema:name"},"generatedOn": {"@id": "schema:name#generatedOn","@type": "schema:name"},"publishedOn": {"@id": "schema:name#publishedOn","@type": "schema:name"},"metadata": "@nest","context": {"@id": "http://schema.org/context","@nest": "metadata","cData": {"identifier": "schema:identifier","name": "schema:name","framework": "schema:framework","board": "schema:board","medium": "schema:medium","subject": "schema:subject","gradeLevel": "schema:gradeLevel"}},"linkedTo": { "@id": "http://schema.org/linkedTo","@nest": "metadata","@context": {"identifier": "http://schema.org/identifier","primaryCategory": "schema:primaryCategory","children": {  "@id": "http://schema.org/linkedToChildren",  "@context": {    "identifier": "http://schema.org/identifier",    "primaryCategory": "schema:primaryCategory"  }}}}}}"""

    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, contextResponse))

   val contextFields =  new TestDialcodeContextUpdater().getContextData(mockHttpUtil, jobConfig).keySet.toList
   val cDataFields =  new TestDialcodeContextUpdater().getContextData(mockHttpUtil, jobConfig)("cData").asInstanceOf[Map[String, AnyRef]].keySet.toList
    println("DialcodeContextUpdaterSpec:: getContextData:: contextFields: " + contextFields)
    println("DialcodeContextUpdaterSpec:: getContextData:: cDataFields: " + cDataFields)

    assert(contextFields.nonEmpty)
  }
  

}

class TestDialcodeContextUpdater extends DialcodeContextUpdater {}