package org.sunbird.job.postpublish.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util.UUID

class ShallowCopyPublishFunction(config: PostPublishProcessorConfig)
                                (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ShallowCopyPublishFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(metadata: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
    val epochTime = System.currentTimeMillis
    val event = s"""{"eid":"BE_JOB_REQUEST","ets":${epochTime},"mid":"LP.${epochTime}.${UUID.randomUUID()}","actor":{"id":"collection-publish","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"sunbird","env":"sunbirddev"},"object":{"ver":"${metadata.pkgVersion}","id":"${metadata.identifier}"},"edata":{"publish_type":"public","metadata":{"identifier":"${metadata.identifier}", "mimeType":"${metadata.mimeType}","objectType":"Collection","lastPublishedBy":"System","pkgVersion":${metadata.pkgVersion}},"action":"publish","iteration":1,"contentType":"${metadata.contentType}"}}"""
    context.output(config.publishEventOutTag, event)
    metrics.incCounter(config.shallowCopyCount)
    logger.info("Shallow copy content publish triggered for " + metadata.identifier)
    logger.info("Shallow copy content publish event: " + event)
  }

  override def metricsList(): List[String] = {
    List(config.shallowCopyCount)
  }
}
