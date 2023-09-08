package org.sunbird.job.livevideostream.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.livevideostream.domain.Event
import org.sunbird.job.livevideostream.service.LiveVideoStreamService
import org.sunbird.job.livevideostream.task.LiveVideoStreamGeneratorConfig
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import java.util

class LiveVideoStreamGenerator(config: LiveVideoStreamGeneratorConfig, httpUtil: HttpUtil)
                              (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                               stringTypeInfo: TypeInformation[String])
  extends BaseProcessKeyedFunction[String, Event, Event](config) {

  implicit lazy val videoStreamConfig: LiveVideoStreamGeneratorConfig = config
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[LiveVideoStreamGenerator])
  private var videoStreamService: LiveVideoStreamService = _
  private lazy val timerDurationInMS: Long = config.timerDuration * 1000
  private var nextTimerTimestamp = 0L

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.skippedEventCount, config.successEventCount, config.failedEventCount, config.retryEventCount)
  }

  override def open(parameters: Configuration, metrics: Metrics): Unit = {
    videoStreamService = new LiveVideoStreamService()(config, httpUtil)
    val processing = videoStreamService.readFromDB(Map("status" -> "PROCESSING")) ++ videoStreamService.readFromDB(Map("status" -> "FAILED", "iteration" -> Map("type" -> "lte", "value" -> 10)))
    if (processing.nonEmpty) {
      logger.info("Requests in queue to validate update the status: " + processing.size)
      videoStreamService.processJobRequest(metrics)
    } else logger.info("open() ==> There are no video streaming requests in queue to validate update the status.")
  }

  override def close(): Unit = {
    videoStreamService.closeConnection()
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event,
                              context: KeyedProcessFunction[String, Event, Event]#Context,
                              metrics: Metrics): Unit = {
    try {
      metrics.incCounter(config.totalEventsCount)
      if (event.isValid) {
        val eData = event.eData ++ Map("channel" -> event.channel)
        videoStreamService.submitJobRequest(eData)
        logger.info("Streaming job submitted for " + event.identifier + " with url: " + event.artifactUrl)
        registerTimer(context)
      } else metrics.incCounter(config.skippedEventCount)
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, Event]#OnTimerContext, metrics: Metrics): Unit = {
    unregisterTimer()
    val processing = videoStreamService.readFromDB(Map("status" -> "PROCESSING")) ++ videoStreamService.readFromDB(Map("status" -> "FAILED", "iteration" -> Map("type" -> "lte", "value" -> 10)))
    if (processing.nonEmpty) {
      logger.info("Requests in queue to validate update the status: " + processing.size)
      videoStreamService.processJobRequest(metrics)
      registerTimer(ctx)
    } else {
      logger.info("There are no video streaming requests in queue to validate update the status.")
    }
  }

  private def registerTimer(context: KeyedProcessFunction[String, Event, Event]#Context): Unit = {
    if (nextTimerTimestamp == 0L) {
      val nextTrigger = context.timestamp() + timerDurationInMS
      context.timerService().registerProcessingTimeTimer(nextTrigger)
      nextTimerTimestamp = nextTrigger
      logger.info("Timer registered to execute at " + nextTimerTimestamp)
    } else {
      logger.info("Timer already exists at: " + nextTimerTimestamp)
    }
  }

  private def unregisterTimer(): Unit = {
    nextTimerTimestamp = 0L
  }

}
