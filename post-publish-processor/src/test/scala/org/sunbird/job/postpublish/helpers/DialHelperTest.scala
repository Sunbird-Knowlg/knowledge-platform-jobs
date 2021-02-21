package org.sunbird.job.postpublish.helpers

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.when
import org.mockito.Mockito.doNothing
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.Metrics
import org.sunbird.job.models.ExtDataConfig
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._

class DialHelperTest extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

    implicit val neo4jUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
    implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    implicit var cassandraUtil: CassandraUtil = _
    val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
    implicit val jobConfig: PostPublishProcessorConfig = new PostPublishProcessorConfig(config)
    implicit val extConfig: ExtDataConfig = new ExtDataConfig(jobConfig.dialcodeKeyspaceName, jobConfig.dialcodeTableName)
    implicit val metrics: Metrics = new Metrics(null)


    override protected def beforeAll(): Unit = {
        super.beforeAll()
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
        cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
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


    "fetchExistingReservedDialcodes" should "return map of dialcodes that is reserved" in {
        val dialUtility = new TestDialHelper()
        when(neo4jUtil.getNodeProperties(ArgumentMatchers.anyString())).thenReturn(getNeo4jData())
        val dialcodes: util.Map[String, Integer] = dialUtility.fetchExistingReservedDialcodes(getEvent())
        dialcodes.isEmpty should be(false)
        dialcodes.getOrDefault("Q1I5I3", -1) shouldEqual (0)
    }

    "reserveDialCodes" should "reserve dialcodes and return a map of the same " in {
        val dialUtility = new TestDialHelper()
        when(httpUtil.post(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyMap())).thenReturn(getReserveDialcodeResponse())
        val reserved = dialUtility.reserveDialCodes(getEvent(), jobConfig)
        reserved.isEmpty should be(false)
        reserved.getOrDefault("V9E4D9", -1) shouldEqual (0)
    }

    "reserveDialCodes with error response" should "throw an exception " in {
        val dialUtility = new TestDialHelper()
        when(httpUtil.post(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyMap())).thenReturn(HTTPResponse(400, ""))
        intercept[Exception] {
            dialUtility.reserveDialCodes(getEvent(), jobConfig)
        }
    }

    "updateDIALToObject" should "update the neo4j data" in {
        doNothing().when(neo4jUtil).updateNodeProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString())
        val dialUtility = new TestDialHelper()
        dialUtility.updateDIALToObject("do_234", "Q1I5I3")
    }

    "fetchExistingDialcodes" should "return map of dialcodes that is reserved" in {
        val dialcodes: util.List[String] = new TestDialHelper().fetchExistingDialcodes(getNeo4jData())
        dialcodes.isEmpty should be(false)
        dialcodes.contains("Q1I5I3") should be (true)
    }

    "validatePrimaryCategory" should "validate the PrimaryCategory" in {
        new TestDialHelper().validatePrimaryCategory(new util.HashMap[String, AnyRef](){{put("primaryCategory", "Course")}}) should be (true)
    }

    "validateQR" should "throw exception" in {
        assertThrows[Exception] {new TestDialHelper().validateQR("")}
    }

    "validateQR" should "validate the QR code image with valid Dialcode" in {
        new TestDialHelper().validateQR("Q1I5I3") should be (true)
    }

    "validateQR" should "invalidate the QR code image with invalid Dialcode" in {
        new TestDialHelper().validateQR("Q1I5I2") should be (false)
    }

    "updatDialcodeRecord" should "throw exception because of invalid channel" in {
        assertThrows[Exception] {new TestDialHelper().updatDialcodeRecord("", "channel1", System.currentTimeMillis())}
    }

    "updatDialcodeRecord" should "throw exception because of passing invalid keyspace" in {
        val extConfig = new ExtDataConfig("invalidKeyspace", jobConfig.dialcodeTableName)
        assertThrows[Exception] {new TestDialHelper().updatDialcodeRecord("123", "channel1", 123)(extConfig, cassandraUtil)}
    }

    "updatDialcodeRecord" should "pass by inserting record into table" in {
        (new TestDialHelper().updatDialcodeRecord("Q1I5I2", "channel1", System.currentTimeMillis())) should be (true)
    }

    "createQRGeneratorEvent" should " throw Exception because of passing invalid keyspaces" in {
        val extConfig = new ExtDataConfig("invalidKeyspace", jobConfig.dialcodeTableName)
        assertThrows[Exception] {new TestDialHelper().createQRGeneratorEvent(new util.HashMap[String, AnyRef](), "", null, jobConfig)(metrics, extConfig, cassandraUtil)}

    }


    def getEvent(): util.Map[String, AnyRef] = {
        Map[String, AnyRef]("reservedDialcodes" -> "{\"Q1I5I3\":0}", "identifier" -> "do_234", "channel" -> "in.ekstep").asJava
    }

    def getInvalidEvent(): util.Map[String, AnyRef] = {
        Map[String, AnyRef]("reservedDialcodes" -> "{\"Q1I5I3\":0}", "channel" -> "in.ekstep").asJava
    }

    def getNeo4jData(): util.Map[String, AnyRef] = {
        Map[String, AnyRef]("reservedDialcodes" -> "{\"Q1I5I3\":0}", "identifier" -> "do_234", "dialcodes" -> (List[String]("Q1I5I3")).asJava).asJava
    }

    def getEmptyNeo4jData(): util.Map[String, AnyRef] = Map[String, AnyRef]().asJava

    def getReserveDialcodeResponse() = {
        val body =
            """
              |{
              |    "id": "ekstep.learning.content.dialcode.reserve",
              |    "ver": "3.0",
              |    "ts": "2021-02-03T10:11:05ZZ",
              |    "params": {
              |        "resmsgid": "4f83734c-7b05-4ebb-b01e-121a365d4399",
              |        "msgid": null,
              |        "err": null,
              |        "status": "successful",
              |        "errmsg": null
              |    },
              |    "responseCode": "OK",
              |    "result": {
              |        "count": 1,
              |        "reservedDialcodes": {
              |            "V9E4D9": 0
              |        },
              |        "node_id": "do_234",
              |        "versionKey": "1612347048689"
              |    }
              |}
            """.stripMargin
        HTTPResponse(200, body)
    }

    def delay(time: Long): Unit = {
        try {
            Thread.sleep(time)
        } catch {
            case ex: Exception => print("")
        }
    }
}


class TestDialHelper extends DialHelper {

}