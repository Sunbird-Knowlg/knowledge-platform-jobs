package org.sunbird.job.postpublish.helpers

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito.when
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.models.ExtDataConfig
import org.sunbird.job.postpublish.config.PostPublishConfig
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._


class QRUtilityTest extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {
    override protected def beforeAll(): Unit = {
        super.beforeAll()
    }

    override protected def afterAll(): Unit = {
        super.afterAll()
    }

    implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
    implicit val neo4jUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
    implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
    implicit val publishConfig: PostPublishConfig = new PostPublishConfig(config, "")
    implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)
    implicit val extConfig: ExtDataConfig = ExtDataConfig("dialcodes", "dialcode_table")


    "fetchExistingDialcodes" should "return map of dialcodes that is reserved" in {
        val qrUtility = new TestQRUtility()
        val imageUrlOption = qrUtility.getQRImageUrl("in.ekstep", "Q1I5I3")
        imageUrlOption.getOrElse("").isEmpty should be (false)
        imageUrlOption.getOrElse("") shouldEqual  "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/in.ekstep/0_Q1I5I3.png"
    }

    def getEvent(): util.Map[String, AnyRef] = {
        Map[String, AnyRef]("reservedDialcodes" -> "{\"Q1I5I3\":0}", "identifier" -> "do_234", "channel" -> "in.ekstep").asJava
    }

    def getInvalidEvent(): util.Map[String, AnyRef] = {
        Map[String, AnyRef]("reservedDialcodes" -> "{\"Q1I5I3\":0}", "channel" -> "in.ekstep").asJava
    }
}

class TestQRUtility extends QRUtility {

}
