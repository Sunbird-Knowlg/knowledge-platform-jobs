package org.sunbird.job.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.incredible.processor.JsonKey
import org.sunbird.incredible.processor.store.{AwsStore, AzureStore, ICertStore, StoreConfig}
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.{CertificateGeneratorFunction}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}


class CertificateGeneratorStreamTask(config: CertificateGeneratorConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val source = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)

    val processStreamTask = env.addSource(source).name(config.certificateGeneratorConsumer)
      .uid(config.certificateGeneratorConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new CertificateGeneratorFunction(config))
      .name("certificate-generator").uid("certificate-generator")
      .setParallelism(config.parallelism)

    processStreamTask.getSideOutput(config.failedEventOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.certificateGeneratorFailedEventProducer)
      .uid(config.certificateGeneratorFailedEventProducer)

    processStreamTask.getSideOutput(config.postCertificateProcessEventOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(config.kafkaPostCertificateProcessEventTopic))
      .name(config.postCertificateProcessEventProducer)
      .uid(config.postCertificateProcessEventProducer)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CertificateGeneratorStreamTask {
  var httpUtil = new HttpUtil
  var certStore: ICertStore = _

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("certificate-generator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val certificateGeneratorConfig = new CertificateGeneratorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(certificateGeneratorConfig)
    val task = new CertificateGeneratorStreamTask(certificateGeneratorConfig, kafkaUtil)
    task.process()
  }

  @throws[Exception]
  def getStorageService(config: CertificateGeneratorConfig): ICertStore = {
    if (null == certStore) {
      println("certificate is null")
      val storageType: String = config.storageType
      val storeParams = new java.util.HashMap[String, AnyRef]
      storeParams.put(JsonKey.TYPE, storageType)
      if (StringUtils.equalsIgnoreCase(storageType, JsonKey.AZURE)) {
        val azureParams = new java.util.HashMap[String, String]
        azureParams.put(JsonKey.containerName, config.containerName)
        azureParams.put(JsonKey.ACCOUNT, config.azureStorageKey)
        azureParams.put(JsonKey.KEY, config.azureStorageSecret)
        storeParams.put(JsonKey.AZURE, azureParams)
        val storeConfig = new StoreConfig(storeParams)
        certStore = new AzureStore(storeConfig)
      } else if (StringUtils.equalsIgnoreCase(storageType, JsonKey.AWS)) {
        val awsParams = new java.util.HashMap[String, String]
        awsParams.put(JsonKey.containerName, config.containerName)
        awsParams.put(JsonKey.ACCOUNT, config.awsStorageKey)
        awsParams.put(JsonKey.KEY, config.awsStorageSecret)
        storeParams.put(JsonKey.AWS, awsParams)
        val storeConfig = new StoreConfig(storeParams)
        certStore = new AwsStore(storeConfig)

      } else throw new Exception("Error while initialising cloud storage")
    }
    certStore
  }
}

// $COVERAGE-ON$
