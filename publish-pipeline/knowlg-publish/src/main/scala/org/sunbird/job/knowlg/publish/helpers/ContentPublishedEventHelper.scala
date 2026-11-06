package org.sunbird.job.knowlg.publish.helpers

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.knowlg.publish.domain.Event
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.publish.core.ObjectData

import java.util.UUID
import scala.collection.JavaConverters._

/**
 * Helper trait, shared by every publish function (Content, Collection, Question, QuestionSet).
 * Emits exactly one generic `content-published` event on every successful publish, always —
 * whether or not the node has any `enrichmentTypes` set. `edata.enrichmentTypes` carries the
 * node's own array verbatim, `[]` when absent/empty. No eligibility/mimeType gate here on
 * purpose — this topic is generic across every possible enrichment type and consumer, so it
 * isn't this producer's job to decide what's interesting to some downstream workflow it knows
 * nothing about. Each consumer (its own Kafka trigger adapter) checks the array for what it
 * actually wants.
 */
trait ContentPublishedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentPublishedEventHelper])

  def pushContentPublishedEvent(obj: ObjectData, objectType: String, config: KnowlgPublishConfig, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    try {
      val enrichmentTypes: List[String] = obj.metadata.getOrElse("enrichmentTypes", List.empty[String]) match {
        case l: java.util.List[_] => l.asScala.toList.map(_.toString)
        case l: List[_] => l.map(_.toString)
        case _ => List.empty[String]
      }
      val event = getContentPublishedEvent(obj, objectType, enrichmentTypes)
      context.output(config.contentPublishedEventOutTag, event)
      metrics.incCounter(config.contentPublishedEventCount)
      logger.info(s"Content published event emitted for ${obj.identifier}, objectType: $objectType, enrichmentTypes: $enrichmentTypes")
    } catch {
      case e: Exception =>
        logger.error(s"Error pushing content published event for ${obj.identifier}: ${e.getMessage}", e)
    }
  }

  def getContentPublishedEvent(obj: ObjectData, objectType: String, enrichmentTypes: List[String]): String = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.$ets.${UUID.randomUUID}"""
    val channelId = obj.getString("channel", "")
    val status = obj.getString("status", "")
    val artifactHash = obj.getString("artifactHash", "")
    val prevArtifactHash = obj.getString("prevArtifactHash", "")
    val enrichmentTypesJson = enrichmentTypes.map(t => s""""$t"""").mkString("[", ",", "]")
    val event = s"""{"eid":"BE_JOB_REQUEST", "ets": $ets, "mid": "$mid", "actor": {"id": "knowlg-publish", "type": "System"}, "edata": {"action":"content-published","identifier":"${obj.identifier}","objectType":"$objectType","mimeType":"${obj.mimeType}","channel":"$channelId","status":"$status","artifactHash":"$artifactHash","prevArtifactHash":"$prevArtifactHash","enrichmentTypes":$enrichmentTypesJson}}""".stripMargin
    logger.info(s"Content Published Event for identifier ${obj.identifier}, objectType $objectType, enrichmentTypes $enrichmentTypes is: $event")
    event
  }
}
