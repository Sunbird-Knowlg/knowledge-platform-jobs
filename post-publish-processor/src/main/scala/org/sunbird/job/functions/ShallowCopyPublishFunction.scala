package org.sunbird.job.functions

import java.lang.reflect.Type
import java.util.UUID

import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.CassandraUtil

class ShallowCopyPublishFunction (config: PostPublishProcessorConfig)
                                  (implicit val stringTypeInfo: TypeInformation[String],
                                   @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ShallowCopyPublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(eData: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val shallowCopied = eData.getOrDefault("shallowCopied", List()).asInstanceOf[List[PublishMetadata]]
    logger.info("Shallow copied contents to publish:" + shallowCopied.size)
    shallowCopied.map(metadata => {
      val epochTime = System.currentTimeMillis
      val event = s"""{"eid":"BE_JOB_REQUEST","ets":${epochTime},"mid":"LP.${epochTime}.${UUID.randomUUID()}","actor":{"id":"Publish Samza Job","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"sunbird","env":"sunbirddev"},"object":{"ver":"${metadata.pkgVersion}","id":"${metadata.identifier}"},"edata":{"publish_type":"public","metadata":{"mimeType":"${metadata.mimeType}","lastPublishedBy":"System","pkgVersion":${metadata.pkgVersion},"action":"publish","iteration":1,"contentType":"${metadata.contentType}"}}}"""
      context.output(config.publishEventOutTag, event)
      event
    }).toList
  }

  override def metricsList(): List[String] = {
    List()
  }
}
