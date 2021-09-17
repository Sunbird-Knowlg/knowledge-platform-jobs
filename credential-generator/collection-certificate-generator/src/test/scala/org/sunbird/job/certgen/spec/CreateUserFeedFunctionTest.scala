package org.sunbird.job.certgen.spec

import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.Metrics
import org.sunbird.job.certgen.functions.{CreateUserFeedFunction, UserFeedMetaData}
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

class CreateUserFeedFunctionTest extends BaseTestSpec {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  val config: Config = ConfigFactory.load("test.conf")
  val userFeedConfig: CertificateGeneratorConfig = new CertificateGeneratorConfig(config)
  val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {
    {
      put(userFeedConfig.userFeedCount, new AtomicLong())
    }
  })


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Clear the metrics

    BaseMetricsReporter.gaugeMetrics.clear()
    when(mockHttpUtil.post(any[String], any[String], any[Map[String, String]]())).thenReturn(HTTPResponse(200, """{"id":"v2.notification.send","ver":"v1","ts":"2020-10-30 13:20:54:940+0000","params":{"resmsgid":null,"msgid":"518d3404-cf1f-4001-81a5-0c58647b32fe","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":"SUCCESS"}}"""))
  }


  override protected def afterAll(): Unit = {
    super.afterAll()

  }

  "UserFeedFunction " should "should send user feed" in {
    implicit val userFeedMetaTypeInfo: TypeInformation[UserFeedMetaData] = TypeExtractor.getForClass(classOf[UserFeedMetaData])
    new CreateUserFeedFunction(userFeedConfig, mockHttpUtil).processElement(UserFeedMetaData("userId", "CourseName", new Date(), "courseId", 0, 0), null, metrics)
    metrics.get(s"${userFeedConfig.userFeedCount}") should be(1)

  }
}
