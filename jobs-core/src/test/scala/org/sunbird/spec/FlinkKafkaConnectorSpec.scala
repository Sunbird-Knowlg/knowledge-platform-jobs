package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.connector.FlinkKafkaConnector

class FlinkKafkaConnectorSpec extends FlatSpec with Matchers {

  val config: Config = ConfigFactory.load("base-test.conf")
  val baseConfig: BaseJobConfig = new BaseJobConfig(config, "test-connector-job")
  val connector = new FlinkKafkaConnector(baseConfig)

  "FlinkKafkaConnector" should "create a KafkaSource for Map type" in {
    val source = connector.kafkaMapSource("test-topic")
    source should not be null
  }

  it should "create a KafkaSink for Map type" in {
    val sink = connector.kafkaMapSink("test-topic")
    sink should not be null
  }

  it should "create a KafkaSource for String type" in {
    val source = connector.kafkaStringSource("test-topic")
    source should not be null
  }

  it should "create a KafkaSink for String type" in {
    val sink = connector.kafkaStringSink("test-topic")
    sink should not be null
  }

  it should "create a KafkaSource for JobRequest type" in {
    val source = connector.kafkaJobRequestSource[TestJobRequest]("test-topic")
    source should not be null
  }

  it should "create a KafkaSink for JobRequest type" in {
    val sink = connector.kafkaJobRequestSink[TestJobRequest]("test-topic")
    sink should not be null
  }
}
