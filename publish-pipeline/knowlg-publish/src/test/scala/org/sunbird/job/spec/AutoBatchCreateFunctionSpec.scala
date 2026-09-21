package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.function.AutoBatchCreateFunction
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil}

class AutoBatchCreateFunctionSpec extends FlatSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)

  def eData(): java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]() {
    {
      put("identifier", "do_11300581751853056099")
      put("name", "Test Course")
      put("createdBy", "user1")
    }
  }

  "AutoBatchCreateFunction" should "increment the success and attempted counters when the LMS API returns 200" in {
    val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.post(anyString(), anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(200, "{}"))
    val fn = new AutoBatchCreateFunction(jobConfig, mockHttpUtil)
    val metrics = fn.registerMetrics(fn.metricsList())

    fn.processElement(eData(), null, metrics)

    metrics.get(jobConfig.autoBatchCreationCount) should be(1)
    metrics.get(jobConfig.autoBatchCreationSuccessCount) should be(1)
    metrics.get(jobConfig.autoBatchCreationFailedCount) should be(0)
  }

  it should "increment the failed counter and swallow the exception when the LMS API returns a non-200 response" in {
    val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.post(anyString(), anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(500, "error"))
    val fn = new AutoBatchCreateFunction(jobConfig, mockHttpUtil)
    val metrics = fn.registerMetrics(fn.metricsList())

    noException should be thrownBy fn.processElement(eData(), null, metrics)

    metrics.get(jobConfig.autoBatchCreationCount) should be(1)
    metrics.get(jobConfig.autoBatchCreationSuccessCount) should be(0)
    metrics.get(jobConfig.autoBatchCreationFailedCount) should be(1)
  }
}
