package org.sunbird.spec

import org.apache.flink.configuration.Configuration
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class BaseTestSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar with BeforeAndAfterEach {

  def testConfiguration(): Configuration = {
    val config = new Configuration()
    config.setString("metrics.reporters", "job_metrics_reporter")
    config.setString("metrics.reporter.job_metrics_reporter.factory.class", classOf[BaseMetricsReporterFactory].getName)
    config
  }

}
