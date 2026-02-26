package org.sunbird.spec

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import net.manub.embeddedkafka.EmbeddedKafka._
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.util.FlinkUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BaseProcessFunctionTestSpec extends BaseSpec with Matchers {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val config: Config = ConfigFactory.load("base-test.conf")
  val bsConfig = new BaseProcessTestConfig(config)
  val gson = new Gson()

  val kafkaConnector = new FlinkKafkaConnector(bsConfig)

  val EVENT_WITH_MESSAGE_ID: String =
    """
      |{"id":"sunbird.telemetry","ver":"3.0","ets":1529500243591,"params":{"msgid":"3fc11963-04e7-4251-83de-18e0dbb5a684",
      |"requesterId":"","did":"a3e487025d29f5b2cd599a8817ac16b8f3776a63","key":""},"events":[{"eid":"LOG","ets":1529499971358,
      |"ver":"3.0","mid":"LOG:5f3c177f90bd5833deade577cc28cbb6","actor":{"id":"159e93d1-da0c-4231-be94-e75b0c226d7c",
      |"type":"user"},"context":{"channel":"b00bc992ef25f1a9a8d63291e20efc8d","pdata":{"id":"local.sunbird.portal",
      |"ver":"0.0.1"},"env":"content-service","sid":"PCNHgbKZvh6Yis8F7BxiaJ1EGw0N3L9B","did":"cab2a0b55c79d12c8f0575d6397e5678",
      |"cdata":[],"rollup":{"l1":"ORG_001","l2":"0123673542904299520","l3":"0123673689120112640",
      |"l4":"b00bc992ef25f1a9a8d63291e20efc8d"}},"object":{},"tags":["b00bc992ef25f1a9a8d63291e20efc8d"],
      |"edata":{"type":"api_access","level":"INFO","message":"","params":[{"url":"/content/composite/v1/search"},
      |{"protocol":"https"},{"method":"POST"},{}]}}],"mid":"56c0c430-748b-11e8-ae77-cd19397ca6b0","syncts":1529500243955}
      |""".stripMargin

  val SHARE_EVENT: String =
    """
      |{"ver":"3.0","eid":"SHARE","ets":1577278681178,"actor":{"type":"User","id":"7c3ea1bb-4da1-48d0-9cc0-c4f150554149"},
      |"context":{"channel":"505c7c48ac6dc1edc9b08f21db5a571d","pdata":{"id":"prod.sunbird.desktop","pid":"sunbird.app",
      |"ver":"2.3.162"},"env":"app","sid":"82e41d87-e33f-4269-aeae-d56394985599","did":"1b17c32bad61eb9e33df281eecc727590d739b2b"},
      |"edata":{"dir":"In","type":"File","items":[{"origin":{"id":"1b17c32bad61eb9e33df281eecc727590d739b2b","type":"Device"},
      |"id":"do_312785709424099328114191","type":"CONTENT","ver":"1","params":[{"transfers":0,"size":21084308}]},
      |{"origin":{"id":"1b17c32bad61eb9e33df281eecc727590d739b2b","type":"Device"},"id":"do_31277435209002188818711",
      |"type":"CONTENT","ver":"18","params":[{"transfers":12,"size":"123"}]},{"origin":{"id":"1b17c32bad61eb9e33df281eecc727590d739b2b",
      |"type":"Device"},"id":"do_31278794857559654411554","type":"TextBook","ver":"1"}]},"object":{"id":"do_312528116260749312248818",
      |"type":"TextBook","version":"10","rollup":{}},"mid":"02ba33e5-15fe-4ec5-b32","syncts":1577278682630,
      |"@timestamp":"2019-12-25T12:58:02.630Z","type":"events"}
      |""".stripMargin

  val EVENT_WITHOUT_DID: String =
    """
      |{"actor":{"id":"org.ekstep.learning.platform","type":"User"},"eid":"LOG","edata":{"level":"ERROR","type":"system",
      |"message":"Exception Occured While Reading event from kafka topic : sunbirddev.system.command. Exception is :
      |java.lang.IllegalStateException: This consumer has already been closed."},"ver":"3.0","syncts":1.586994119534E12,
      |"ets":1.586994119534E12,"context":{"channel":"in.ekstep","pdata":{"id":"dev.sunbird.learning.platform",
      |"pid":"learning-service","ver":"1.0"},"env":"system"},"flags":{"pp_duplicate_skipped":true,"pp_validation_processed":true},
      |"mid":"LP.1586994119534.4bfe9b31-216d-46ea-8e60-d7ea1b1a103c","type":"events"}
    """.stripMargin

  val JOB_REQUEST_EVENT: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1608027159,"mid":"LP.1608027159.e8545bbd-20e6-43d8-85f5-a854680ad373",
      |"actor":{"id":"Post Publish Processor","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},
      |"channel":"01246376237871104093","env":"prod"},"object":{"ver":"1604925386745","id":"do_31314751443052134413666"},
      |"edata":{"trackable":{"enabled":"Yes","autoBatch":"No"},"identifier":"do_31314751443052134413666","createdFor":["01246376237871104093"],
      |"createdBy":"f633f462-1b78-4a9f-bdfb-3b7c57808f89","name":"Physical Education",
      |"action":"post-publish-process","iteration":1,"id":"do_31314751443052134413666","mimeType":"application/vnd.ekstep.content-collection",
      |"contentType":"Course","pkgVersion":1,"status":"Live"}}
      |""".stripMargin

  val customKafkaConsumerProperties: Map[String, String] =
    Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-event-schema-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )
  implicit val deserializer: StringDeserializer = new StringDeserializer()

  override def beforeAll(): Unit = {
    super.beforeAll()

    EmbeddedKafka.start()(embeddedKafkaConfig)
    createTestTopics(bsConfig.testTopics)

    publishStringMessageToKafka(bsConfig.kafkaMapInputTopic, EVENT_WITH_MESSAGE_ID)
    publishStringMessageToKafka(bsConfig.kafkaStringInputTopic, SHARE_EVENT)
    publishStringMessageToKafka(bsConfig.kafkaJobReqInputTopic, JOB_REQUEST_EVENT)

    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(topics: List[String]): Unit = {
    topics.foreach(createCustomTopic(_))
  }

  ignore should "validate serialization and deserialization of Map, String and Event schema" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(bsConfig)


    val mapStream =
      env.fromSource(kafkaConnector.kafkaMapSource(bsConfig.kafkaMapInputTopic), WatermarkStrategy.noWatermarks(), "map-event-consumer").name("map-event-consumer")
        .process(new TestMapStreamFunc(bsConfig)).name("TestMapEventStream")

    mapStream.getSideOutput(bsConfig.mapOutputTag)
      .sinkTo(kafkaConnector.kafkaMapSink(bsConfig.kafkaMapOutputTopic))
      .name("Map-Event-Producer")

    val stringStream =
      env.fromSource(kafkaConnector.kafkaStringSource(bsConfig.kafkaStringInputTopic), WatermarkStrategy.noWatermarks(), "string-event-consumer").name("string-event-consumer")
        .process(new TestStringStreamFunc(bsConfig)).name("TestStringEventStream")

    stringStream.getSideOutput(bsConfig.stringOutputTag)
      .sinkTo(kafkaConnector.kafkaStringSink(bsConfig.kafkaStringOutputTopic))
      .name("String-Producer")

    val jobReqStream =
      env.fromSource(kafkaConnector.kafkaJobRequestSource[TestJobRequest](bsConfig.kafkaJobReqInputTopic), WatermarkStrategy.noWatermarks(), "job-request-event-consumer").name("job-request-event-consumer")
        .process(new TestJobRequestStreamFunc(bsConfig)).name("TestJobRequestEventStream")

    jobReqStream.getSideOutput(bsConfig.jobRequestOutputTag)
      .sinkTo(kafkaConnector.kafkaJobRequestSink[TestJobRequest](bsConfig.kafkaJobReqOutputTopic))
      .name("JobRequest-Event-Producer")

    Future {
      env.execute("TestSerDeFunctionality")
    }

    val mapSchemaMessages = consumeNumberMessagesFrom[String](bsConfig.kafkaMapOutputTopic, 1)
    val stringSchemaMessages = consumeNumberMessagesFrom[String](bsConfig.kafkaStringOutputTopic, 1)
    val jobRequestSchemaMessages = consumeNumberMessagesFrom[String](bsConfig.kafkaJobReqOutputTopic, 1)

    mapSchemaMessages.size should be(1)
    stringSchemaMessages.size should be(1)
    jobRequestSchemaMessages.size should be(1)
  }

}
