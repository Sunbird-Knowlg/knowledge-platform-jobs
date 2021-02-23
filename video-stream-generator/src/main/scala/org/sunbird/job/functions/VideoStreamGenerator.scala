package org.sunbird.job.functions

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.Event
import org.sunbird.job.service.VideoStreamService
import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

class VideoStreamGenerator(config: VideoStreamGeneratorConfig, httpUtil:HttpUtil)
                          (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                           stringTypeInfo: TypeInformation[String])
                          extends BaseProcessKeyedFunction[String, Event, Event](config) {

    implicit lazy val videoStreamConfig: VideoStreamGeneratorConfig = config
    private[this] lazy val logger = LoggerFactory.getLogger(classOf[VideoStreamGenerator])
    private var videoStreamService:VideoStreamService = _
    lazy val timerDurationInMS: Long = config.timerDuration * 1000

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.skippedEventCount, config.successEventCount, config.failedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        videoStreamService = new VideoStreamService()(config, httpUtil);
    }

    override def close(): Unit = {
        videoStreamService.closeConnection()
        super.close()
    }

    override def processElement(event: Event,
                                context: KeyedProcessFunction[String, Event, Event]#Context,
                                metrics: Metrics): Unit = {
        metrics.incCounter(config.totalEventsCount)
        if (event.isValid) {
          videoStreamService.submitJobRequest(event.eData)
          logger.info("Streaming job submitted for " + event.identifier + " with url: " + event.artifactUrl)
          val nextTimerTimestamp = context.timestamp() + timerDurationInMS
          context.timerService().registerProcessingTimeTimer(nextTimerTimestamp)
          logger.info("Timer registered to execute at " + nextTimerTimestamp)
        } else metrics.incCounter(config.skippedEventCount)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, Event]#OnTimerContext, metrics: Metrics): Unit = {
        val processing = videoStreamService.readFromDB(Map("status" -> "PROCESSING")) ++ videoStreamService.readFromDB(Map("status" -> "FAILED", "iteration" -> Map("type" -> "lte", "value" -> 10)))
        if (processing.nonEmpty) {
          logger.info("Requests in queue to validate update the status: " + processing.size)
          videoStreamService.processJobRequest(metrics)
          val nextTimerTimestamp = ctx.timestamp() + timerDurationInMS
          ctx.timerService().registerProcessingTimeTimer(nextTimerTimestamp)
          logger.info("Timer registered to execute at " + nextTimerTimestamp)
        } else {
          logger.info("There are no video streaming requests in queue to validate update the status.")
        }
    }

}
