package org.sunbird.job.functions

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.LoggerFactory
import org.sunbird.job.{Metrics, TimeWindowBaseProcessFunction}
import org.sunbird.job.service.VideoStreamService
import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.job.util.HttpUtil

class VideoStreamUrlUpdator(config: VideoStreamGeneratorConfig)
                          (implicit val stringTypeInfo: TypeInformation[String],
                           implicit val httpUtil: HttpUtil)
  extends TimeWindowBaseProcessFunction[String, String, String](config) {

  implicit lazy val videoStreamConfig: VideoStreamGeneratorConfig = config
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[VideoStreamGenerator])
  private var videoStreamService: VideoStreamService = _

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    videoStreamService = new VideoStreamService();
  }

  override def close(): Unit = {
    videoStreamService.closeConnection()
    super.close()
  }

  override def process(key: String,
                       context: ProcessWindowFunction[String, String, String, TimeWindow]#Context,
                       events: Iterable[String],
                       metrics: Metrics): Unit = {
    videoStreamService.processJobRequest(metrics)
  }
}
