package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.fixture.EventFixture
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.util.FlinkUtil

class FlinkUtilSpec extends FlatSpec with Matchers {

  "FlinkUtil" should "create execution context with distributed checkpointing enabled" in {
    val customConf = ConfigFactory.parseString(EventFixture.customConfig)
    val flinkConfig: BaseJobConfig = new BaseJobConfig(customConf, "test-job")
    val context: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(flinkConfig)
    context should not be null
  }

  it should "create execution context without distributed checkpointing" in {
    val config: Config = ConfigFactory.load("base-test.conf")
    val flinkConfig: BaseJobConfig = new BaseJobConfig(config, "test-job-no-dist-ckpt")
    val context: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(flinkConfig)
    context should not be null
  }
}
