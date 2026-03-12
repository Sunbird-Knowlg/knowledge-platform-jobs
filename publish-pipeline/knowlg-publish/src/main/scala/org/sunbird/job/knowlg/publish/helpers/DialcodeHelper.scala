package org.sunbird.job.knowlg.publish.helpers

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.ScalaJsonUtil

import java.util.UUID
import scala.collection.mutable

/**
 * Helper trait to push DIAL code related events to the appropriate Kafka topics.
 * The actual processing of these events is handled by downstream jobs:
 *   - dialcode-context-updater: handles context updates
 *   - qrcode-image-generator: handles QR image generation
 */
trait DialcodeHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DialcodeHelper])

  /**
   * For content (non-collection) objects that have dialcodes:
   * Pushes a dialcode context update event to the dialcode-context-updater topic.
   * Pushes a QR image generation event to the qrimage topic.
   */
  def pushDIALcodeContextUpdaterEvent(obj: ObjectData, config: KnowlgPublishConfig, context: ProcessFunction[_, String]#Context)(implicit metrics: Metrics): Unit = {
    val nodeId: String = obj.identifier.replaceAll(".img", "")
    val draftDialcodes: List[String] = obj.metadata.getOrElse("dialcodes", List.empty[String]).asInstanceOf[List[String]]

    if (draftDialcodes.nonEmpty) {
      draftDialcodes.foreach { dialcode =>
        // Push to dialcode-context-updater
        val contextEvent = getDIALcodeContextUpdateEvent(obj, nodeId, dialcode, "dialcode-context-update", config)
        context.output(config.dialcodeContextUpdaterOutTag, contextEvent)
        metrics.incCounter(config.dialcodeContextUpdaterEventCount)
        logger.info(s"DialcodeHelper:: Pushed dialcode context update event for dialcode: $dialcode, identifier: $nodeId")

        // Push to qrimage generator
        val qrEvent = getQRImageEvent(obj, dialcode, config)
        context.output(config.qrimageOutTag, qrEvent)
        logger.info(s"DialcodeHelper:: Pushed QR image generation event for dialcode: $dialcode, identifier: $nodeId")
      }
    }
  }

  /**
   * For collection objects with hierarchy DIAL codes:
   * Pushes dialcode context update events for all added/removed DIAL codes.
   * Pushes QR image generation events.
   */
  def pushCollectionDIALcodeEvents(obj: ObjectData, dialContextMap: Map[String, AnyRef], config: KnowlgPublishConfig, context: ProcessFunction[_, String]#Context)(implicit metrics: Metrics): Unit = {
    val addContextDialCodes = dialContextMap.getOrElse("addContextDialCodes", mutable.Map.empty)
      .asInstanceOf[mutable.Map[List[String], String]]

    val removeContextDialCodes = dialContextMap.getOrElse("removeContextDialCodes", mutable.Map.empty)
      .asInstanceOf[mutable.Map[List[String], String]]

    // Push context update events for added DIAL codes
    addContextDialCodes.foreach { case (dialcodes, unitId) =>
      dialcodes.foreach { dialcode =>
        val contextEvent = getDIALcodeContextUpdateEvent(obj, unitId, dialcode, "dialcode-context-update", config)
        context.output(config.dialcodeContextUpdaterOutTag, contextEvent)
        metrics.incCounter(config.dialcodeContextUpdaterEventCount)
        logger.info(s"DialcodeHelper:: Pushed dialcode context update event for dialcode: $dialcode, unitId: $unitId")

        // Push QR image generation event
        val qrEvent = getQRImageEvent(obj, dialcode, config)
        context.output(config.qrimageOutTag, qrEvent)
        logger.info(s"DialcodeHelper:: Pushed QR image generation event for dialcode: $dialcode, unitId: $unitId")
      }
    }

    // Push context delete events for removed DIAL codes
    removeContextDialCodes.foreach { case (dialcodes, unitId) =>
      dialcodes.foreach { dialcode =>
        val contextEvent = getDIALcodeContextUpdateEvent(obj, unitId, dialcode, "dialcode-context-delete", config)
        context.output(config.dialcodeContextUpdaterOutTag, contextEvent)
        metrics.incCounter(config.dialcodeContextUpdaterEventCount)
        logger.info(s"DialcodeHelper:: Pushed dialcode context delete event for dialcode: $dialcode, unitId: $unitId")
      }
    }
  }

  private def getDIALcodeContextUpdateEvent(obj: ObjectData, identifier: String, dialcode: String, action: String, config: KnowlgPublishConfig): String = {
    val ets = System.currentTimeMillis
    val mid = s"LP.$ets.${UUID.randomUUID}"
    val channelId = obj.getString("channel", "")
    val ver = obj.getString("versionKey", "")
    val event =
      s"""{"eid":"BE_JOB_REQUEST","ets":$ets,"mid":"$mid","actor":{"id":"DIAL Code Context Updater","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"$channelId","env":"${config.jobEnv}"},"object":{"ver":"$ver","id":"$identifier"},"edata":{"action":"$action","iteration":1,"dialcode":"$dialcode","identifier":"$identifier","channel":"$channelId"}}"""
    event
  }

  private def getQRImageEvent(obj: ObjectData, dialcode: String, config: KnowlgPublishConfig): String = {
    val ets = System.currentTimeMillis
    val identifier = obj.identifier.replaceAll(".img", "")
    val channelId = obj.getString("channel", "")
    val event =
      s"""{"eid":"BE_QR_IMAGE_GENERATOR","objectId":"$identifier","dialcodes":[{"data":"${config.dialBaseUrl}$dialcode","text":"$dialcode","id":"0_$dialcode"}],"storage":{"container":"${config.dialStorageContainer}","path":"$channelId/","fileName":"${identifier}_$ets"},"config":{"errorCorrectionLevel":"H","pixelsPerBlock":2,"qrCodeMargin":3,"textFontName":"Verdana","textFontSize":11,"textCharacterSpacing":0.1,"imageFormat":"png","colourModel":"Grayscale","imageBorderSize":1}}"""
    event
  }
}
