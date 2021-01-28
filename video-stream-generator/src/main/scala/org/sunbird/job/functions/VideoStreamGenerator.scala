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
import org.sunbird.job.domain.Event
import org.sunbird.job.service.VideoStreamService
import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class VideoStreamGenerator(config: VideoStreamGeneratorConfig)
                          (implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                           implicit val stringTypeInfo: TypeInformation[String],
                           implicit val httpUtil: HttpUtil)
                          extends BaseProcessFunction[Event, String](config) {

    implicit lazy val videoStreamConfig: VideoStreamGeneratorConfig = config
    private[this] lazy val logger = LoggerFactory.getLogger(classOf[VideoStreamGenerator])
    private var videoStreamService:VideoStreamService = _

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.skippedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        videoStreamService = new VideoStreamService();
    }

    override def close(): Unit = {
        videoStreamService.closeConnection()
        super.close()
    }

    override def processElement(event: Event,
                                context: ProcessFunction[Event, String]#Context,
                                metrics: Metrics): Unit = {
        metrics.incCounter(config.totalEventsCount)
        if(event.isValid) {
            logger.info(s"Event eid::${event.eid}:: valid event")
            videoStreamService.submitJobRequest(event.eData)
            context.output(config.videoStreamJobOutput, "processed")
        } else metrics.incCounter(config.skippedEventCount)
    }
}
