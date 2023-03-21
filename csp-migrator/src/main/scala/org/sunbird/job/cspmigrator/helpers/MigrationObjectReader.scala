package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import java.util.UUID

import org.apache.flink.streaming.api.scala.OutputTag

import scala.collection.JavaConverters._

trait MigrationObjectReader {

  private[this] val logger = LoggerFactory.getLogger(classOf[MigrationObjectReader])

  def getMetadata(identifier: String)(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    val metaData = neo4JUtil.getNodeProperties(identifier).asScala.toMap
    val id = metaData.getOrElse("IL_UNIQUE_ID", identifier).asInstanceOf[String]
    val objType = metaData.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String]
    logger.info("MigrationObjectReader:: getMetadata:: identifier: " + identifier + " with objType: " + objType)
    metaData ++ Map[String, AnyRef]("identifier" -> id, "objectType" -> objType) - ("IL_UNIQUE_ID", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE")
  }

  def getContentBody(identifier: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): String = {
    // fetch content body from cassandra
    val selectId = QueryBuilder.select()
    selectId.fcall("blobAsText", QueryBuilder.column("body")).as("body")
    val selectWhereId: Select.Where = selectId.from(config.contentKeyspaceName, config.contentTableName).where().and(QueryBuilder.eq("content_id", identifier))
    logger.info("MigrationObjectReader:: getContentBody:: ECML Body Fetch Query :: " + selectWhereId.toString)
    val rowId = cassandraUtil.findOne(selectWhereId.toString)
    if (null != rowId && null != rowId.getString("body") ) rowId.getString("body") else ""
  }

  def getAssessmentItemData(identifier: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Row = {
    logger.info("MigrationObjectReader ::: getAssessmentItemData ::: Reading Question External Data For : " + identifier)
    val select = QueryBuilder.select()
    val extProps = config.getConfig.getStringList("cassandra_fields_to_migrate.assessmentitem").asScala.toList
    extProps.foreach(prop => select.fcall("blobAsText", QueryBuilder.column(prop.toLowerCase())).as(prop.toLowerCase()))
    val selectWhere: Select.Where = select.from(config.contentKeyspaceName, config.assessmentTableName).where().and(QueryBuilder.eq("question_id", identifier))
    logger.info("MigrationObjectReader ::: getAssessmentItemData:: Cassandra Fetch Query :: " + selectWhere.toString)
    cassandraUtil.findOne(selectWhere.toString)
  }

  def getCollectionHierarchy(identifier: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): String = {
    val selectId = QueryBuilder.select().column("hierarchy")
    val selectWhereId: Select.Where = selectId.from(config.hierarchyKeyspaceName, config.hierarchyTableName).where().and(QueryBuilder.eq("identifier", identifier))
    logger.info("MigrationObjectReader:: getCollectionHierarchy:: Hierarchy Fetch Query :: " + selectWhereId.toString)
    val rowId = cassandraUtil.findOne(selectWhereId.toString)
    if (null != rowId && null != rowId.getString("hierarchy")) rowId.getString("hierarchy") else ""
  }

  def getQuestionSetHierarchy(identifier: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): String = {
    val selectId = QueryBuilder.select().column("hierarchy")
    val selectWhereId: Select.Where = selectId.from(config.qsHierarchyKeyspaceName, config.qsHierarchyTableName).where().and(QueryBuilder.eq("identifier", identifier))
    logger.info("MigrationObjectReader:: getQuestionSetHierarchy:: Hierarchy Fetch Query :: " + selectWhereId.toString)
    val rowId = cassandraUtil.findOne(selectWhereId.toString)
    if (null != rowId && null != rowId.getString("hierarchy")) rowId.getString("hierarchy") else ""
  }


  def pushStreamingUrlEvent(objMetadata: Map[String, AnyRef], context: ProcessFunction[Event, String]#Context, config: CSPMigratorConfig)(implicit metrics: Metrics): Unit = {
    val event = getStreamingEvent(objMetadata, config)
    context.output(config.generateVideoStreamingOutTag, event)
  }

  def getStreamingEvent(objMetadata: Map[String, AnyRef], config: CSPMigratorConfig): String = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.$ets.${UUID.randomUUID}"""
    val channelId = objMetadata.getOrElse("channel", "").asInstanceOf[String]
    val ver = objMetadata.getOrElse("versionKey", "").asInstanceOf[String]
    val artifactUrl = objMetadata.getOrElse("artifactUrl", "").asInstanceOf[String]
    val contentType = objMetadata.getOrElse("contentType", "").asInstanceOf[String]
    val mimeType = objMetadata.getOrElse("mimeType", "").asInstanceOf[String]
    val status = objMetadata.getOrElse("status", "").asInstanceOf[String]
    val identifier = objMetadata.getOrElse("identifier", "").asInstanceOf[String]
    val pkgVersion = objMetadata.getOrElse("pkgVersion", "").asInstanceOf[Number]
    val event = s"""{"eid":"BE_JOB_REQUEST", "ets": $ets, "mid": "$mid", "actor": {"id": "Live Video Stream Generator", "type": "System"}, "context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"}, "channel":"$channelId","env":"${config.jobEnv}"},"object":{"ver":"$ver","id":"$identifier"},"edata": {"action":"live-video-stream-generate","iteration":1,"identifier":"$identifier","channel":"$channelId","artifactUrl":"$artifactUrl","mimeType":"$mimeType","contentType":"$contentType","pkgVersion":$pkgVersion,"status":"$status"}}""".stripMargin
    logger.info(s"MigrationObjectReader :: Asset Video Streaming Event for identifier $identifier  is  : $event")
    event
  }

  def pushLiveNodePublishEvent(objMetadata: Map[String, AnyRef], context: ProcessFunction[Event, String]#Context, metrics: Metrics, config: CSPMigratorConfig, tag: OutputTag[String]): Unit = {
    val epochTime = System.currentTimeMillis
    val identifier = objMetadata.getOrElse("identifier", "").asInstanceOf[String]
    val pkgVersion = objMetadata.getOrElse("pkgVersion", "").asInstanceOf[Number]
    val objectType = objMetadata.getOrElse("objectType", "").asInstanceOf[String]
    val contentType = objMetadata.getOrElse("contentType", "").asInstanceOf[String]
    val mimeType = objMetadata.getOrElse("mimeType", "").asInstanceOf[String]
    val status = objMetadata.getOrElse("status", "").asInstanceOf[String]
    val publishType = if(status.equalsIgnoreCase("Live")) "Public" else "Unlisted"

    val event = s"""{"eid":"BE_JOB_REQUEST","ets":$epochTime,"mid":"LP.$epochTime.${UUID.randomUUID()}","actor":{"id":"content-republish","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"sunbird","env":"${config.jobEnv}"},"object":{"ver":"$pkgVersion","id":"$identifier"},"edata":{"publish_type":"$publishType","metadata":{"identifier":"$identifier", "mimeType":"$mimeType","objectType":"$objectType","lastPublishedBy":"System","pkgVersion":$pkgVersion},"action":"republish","iteration":1,"contentType":"$contentType"}}"""
    context.output(tag, event)
    metrics.incCounter(config.liveContentNodePublishCount)
    logger.info("MigrationObjectReader :: Live content publish triggered for " + identifier)
    logger.info("MigrationObjectReader :: Live content publish event: " + event)
  }

  def pushQuestionPublishEvent(objMetadata: Map[String, AnyRef], context: ProcessFunction[Event, String]#Context, metrics: Metrics, config: CSPMigratorConfig, tag: OutputTag[String], countMetric: String): Unit = {
    val epochTime = System.currentTimeMillis
    val identifier = objMetadata.getOrElse("identifier", "").asInstanceOf[String]
    val pkgVersion = objMetadata.getOrElse("pkgVersion", "").asInstanceOf[Number]
    val objectType = objMetadata.getOrElse("objectType", "").asInstanceOf[String]
    val mimeType = objMetadata.getOrElse("mimeType", "").asInstanceOf[String]
    val status = objMetadata.getOrElse("status", "").asInstanceOf[String]
    val publishType = if(status.equalsIgnoreCase("Live")) "Public" else "Unlisted"
    val channel = objMetadata.getOrElse("channel", "").asInstanceOf[String]
    val lastPublishedBy = objMetadata.getOrElse("lastPublishedBy", "System").asInstanceOf[String]
    val event = s"""{"eid":"BE_JOB_REQUEST","ets":$epochTime,"mid":"LP.$epochTime.${UUID.randomUUID()}","actor":{"id":"question-republish","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"${channel}","env":"${config.jobEnv}"},"object":{"ver":"$pkgVersion","id":"$identifier"},"edata":{"publish_type":"$publishType","metadata":{"identifier":"$identifier", "mimeType":"$mimeType","objectType":"$objectType","lastPublishedBy":"${lastPublishedBy}","pkgVersion":$pkgVersion},"action":"republish","iteration":1}}"""
    context.output(tag, event)
    metrics.incCounter(countMetric)
    logger.info(s"MigrationObjectReader :: Live ${objectType} publish triggered for " + identifier)
    logger.info(s"MigrationObjectReader :: Live ${objectType} publish event: " + event)
  }

}
