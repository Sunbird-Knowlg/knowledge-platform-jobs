package org.sunbird.job.functions

//import java.util.UUID

import java.{lang, util}

import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.json4s.jackson.JsonMethods.parse
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.service.VideoStreamService
import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class VideoStreamGenerator(config: VideoStreamGeneratorConfig)
                          (implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                           implicit val stringTypeInfo: TypeInformation[String])
                          extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {

    implicit lazy val videoStreamConfig: VideoStreamGeneratorConfig = config
    private[this] lazy val logger = LoggerFactory.getLogger(classOf[VideoStreamGenerator])
    private var videoStreamService:VideoStreamService = _

    override def metricsList(): List[String] = {
        List(config.successEventCount, config.failedEventCount, config.batchEnrolmentUpdateEventCount, config.dbUpdateCount, config.dbReadCount, config.cacheHitCount, config.skipEventsCount, config.cacheMissCount)
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        videoStreamService = new VideoStreamService();
    }

    override def close(): Unit = {
        videoStreamService.closeConnection()
        super.close()
    }

    override def processElement(event: util.Map[String, AnyRef],
                         context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
                         metrics: Metrics): Unit = {
        logger.info("Event eid:: "+ event.get("eid"))
        val eData = event.get("edata").asInstanceOf[util.Map[String, AnyRef]].asScala.toMap
        if(isValidEvent(eData)) {
            logger.info("Event eid:: valid event")
            videoStreamService.submitJobRequest(eData)
            context.output(config.videoStreamJobOutput, "processed")
        }
    }

    private def isValidEvent(eData: Map[String, AnyRef]): Boolean = {
        val artifactUrl = eData.getOrElse("artifactUrl", "").asInstanceOf[String]
        val mimeType = eData.getOrElse("mimeType", "").asInstanceOf[String]
        val identifier = eData.getOrElse("identifier", "").asInstanceOf[String]

        StringUtils.isNotBlank(artifactUrl) &&
          StringUtils.isNotBlank(mimeType) &&
          (
            StringUtils.equalsIgnoreCase(mimeType, "video/mp4") ||
              StringUtils.equalsIgnoreCase(mimeType, "video/webm")
            ) &&
          StringUtils.isNotBlank(identifier)
    }
}
