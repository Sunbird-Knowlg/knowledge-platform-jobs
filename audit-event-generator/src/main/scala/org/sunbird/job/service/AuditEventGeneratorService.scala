package org.sunbird.job.service

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.task.AuditEventGeneratorConfig
import org.sunbird.job.util.JSONUtil
import org.sunbird.telemetry.TelemetryGenerator
import org.sunbird.telemetry.TelemetryParams
import org.sunbird.job.domain.Event
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}

import java.util
import java.text.SimpleDateFormat

trait AuditEventGeneratorService {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditEventGeneratorService])
  private val IMAGE_SUFFIX = ".img"
  private val OBJECT_TYPE_IMAGE_SUFFIX = "Image"
  private val SKIP_AUDIT = "{\"object\": {\"type\":null}}"
  private lazy val definitionCache = new DefinitionCache

  private val systemPropsList = List("IL_SYS_NODE_TYPE", "IL_FUNC_OBJECT_TYPE", "IL_UNIQUE_ID", "IL_TAG_NAME", "IL_ATTRIBUTE_NAME", "IL_INDEXABLE_METADATA_KEY", "IL_NON_INDEXABLE_METADATA_KEY",
    "IL_IN_RELATIONS_KEY", "IL_OUT_RELATIONS_KEY", "IL_REQUIRED_PROPERTIES", "IL_SYSTEM_TAGS_KEY", "IL_SEQUENCE_INDEX", "SYS_INTERNAL_LAST_UPDATED_ON", "lastUpdatedOn", "versionKey", "lastStatusChangedOn")

  private def getContext(channelId: String, env: String): Map[String, String] = {
    val context = Map(
      TelemetryParams.ACTOR.name -> "org.ekstep.learning.platform",
      TelemetryParams.CHANNEL.name -> channelId,
      TelemetryParams.ENV.name -> env
    )
    context
  }

  def processEvent(message: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics)(implicit config: AuditEventGeneratorConfig): Unit = {
    logger.info("AUDIT Event::" + JSONUtil.serialize(message))
    logger.info("Input Message Received for : [" + message.nodeUniqueId + "], Txn Event createdOn:" + message.read("createdOn") + ", Operation Type:" + message.operationType)
    try {
      val auditEventStr = getAuditMessage(message)
      val auditMap = JSONUtil.deserialize[Map[String, AnyRef]](auditEventStr)
      val objectType = auditMap.getOrElse("object", null).asInstanceOf[Map[String, AnyRef]].getOrElse("type", null).asInstanceOf[String]
      if (null != objectType) {
        context.output(config.auditOutputTag, auditEventStr)
        logger.info("Telemetry Audit Message Successfully Sent for : " + auditMap.getOrElse("object", null).asInstanceOf[Map[String, AnyRef]].getOrElse("id", "").asInstanceOf[String] + " :: event ::" + auditEventStr)
        metrics.incCounter(config.successEventCount)
      }
      else {
        logger.info("Skipped event as the objectype is not available, event =" + auditEventStr)
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case e: Exception =>
        logger.error("Failed to process message :: " + JSONUtil.serialize(message), e)
        metrics.incCounter(config.failedEventCount)
    }
  }

  def getDefinition(graphId: String, objectType: String)(implicit config: AuditEventGeneratorConfig): ObjectDefinition = {
    try {
      definitionCache.getDefinition(objectType, config.configVersion, config.basePath)
    } catch {
      case ex: Exception => {
        new ObjectDefinition(objectType, config.configVersion, Map[String, AnyRef](), Map[String, AnyRef]())
      }
    }
  }


  def getAuditMessage(message: Event)(implicit config: AuditEventGeneratorConfig): String = {
    var auditMap: String = null
    var objectId = message.nodeUniqueId
    var objectType = message.objectType
    val env = if (null != objectType) objectType.toLowerCase.replace("image", "") else "system"
    val graphId = message.readOrDefault("graphId", "")
    val userId = message.readOrDefault("userId", "")
    val definitionNode: ObjectDefinition = getDefinition(graphId, objectType)

    var channelId = config.defaultChannel
    val channel = message.readOrDefault("channel", null).asInstanceOf[String]
    if (null != channel) channelId = channel

    val transactionData = message.readOrDefault("transactionData", Map[String, AnyRef]())
    val propertyMap = transactionData("properties").asInstanceOf[Map[String, AnyRef]]
    val statusMap = propertyMap.getOrElse("status", null).asInstanceOf[Map[String, AnyRef]]
    val lastStatusChangedOn = propertyMap.getOrElse("lastStatusChangedOn", null).asInstanceOf[Map[String, AnyRef]]
    val addedRelations = transactionData.getOrElse("addedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
    val removedRelations = transactionData.getOrElse("removedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
    var pkgVersion = ""
    val pkgVerMap = propertyMap.getOrElse("pkgVersion", null).asInstanceOf[Map[String, AnyRef]]
    if (null != pkgVerMap) pkgVersion = s"${pkgVerMap.get("nv")}"
    var prevStatus = ""
    var currStatus = ""
    var duration = ""
    if (null != statusMap) {
      prevStatus = statusMap.getOrElse("ov", null).asInstanceOf[String]
      currStatus = statusMap.getOrElse("nv", null).asInstanceOf[String]
      // Compute Duration for Status Change
      if (StringUtils.isNotBlank(currStatus) && StringUtils.isNotBlank(prevStatus) && null != lastStatusChangedOn) {
        var ov = lastStatusChangedOn.getOrElse("ov", null).asInstanceOf[String]
        val nv = lastStatusChangedOn.getOrElse("nv", null).asInstanceOf[String]
        if (null == ov) ov = propertyMap.getOrElse("lastUpdatedOn", null).asInstanceOf[Map[String, AnyRef]].getOrElse("ov", null).asInstanceOf[String]
        if (null != ov && null != nv) duration = String.valueOf(computeDuration(ov, nv))
      }
    }
    var props: List[String] = propertyMap.keys.toList
    props ++= getRelationProps(addedRelations, definitionNode)
    props ++= getRelationProps(removedRelations, definitionNode)
    val propsExceptSystemProps = props.filter(prop => !systemPropsList.contains(prop))
    val cdata = getCData(addedRelations, removedRelations, propertyMap)
    var context: Map[String, String] = getContext(channelId, env)
    objectId = if (null != objectId) objectId.replaceAll(IMAGE_SUFFIX, "") else objectId
    objectType = if (null != objectType) objectType.replaceAll(OBJECT_TYPE_IMAGE_SUFFIX, "") else objectType
    context ++= Map("objectId" -> objectId)
    context ++= Map("objectType" -> objectType)
    if (StringUtils.isNotBlank(duration)) context ++= Map("duration" -> duration)
    if (StringUtils.isNotBlank(pkgVersion)) context ++= Map("pkgVersion" -> pkgVersion)
    if (StringUtils.isNotBlank(userId)) context ++= Map(TelemetryParams.ACTOR.name -> userId)
    if (propsExceptSystemProps.nonEmpty) {
      TelemetryGenerator.setComponent("audit-event-generator")
      val auditMessage = TelemetryGenerator.audit(
        JSONUtil.deserialize[util.Map[String, String]](JSONUtil.serialize(context)),
        JSONUtil.deserialize[util.List[String]](JSONUtil.serialize(propsExceptSystemProps)),
        currStatus,
        prevStatus,
        JSONUtil.deserialize[util.List[util.Map[String, Object]]](JSONUtil.serialize(cdata)))
      logger.info("Audit Message for Content Id [" + objectId + "] : " + auditMessage);
      auditMap = auditMessage
    }
    else {
      logger.info("Skipping Audit log as props is null or empty")
      auditMap = SKIP_AUDIT
    }
    auditMap
  }

  /**
   * @param addedRelations
   * @param removedRelations
   * @param propertyMap
   * @return
   */
  private def getCData(addedRelations: List[Map[String, AnyRef]], removedRelations: List[Map[String, AnyRef]], propertyMap: Map[String, AnyRef]): List[Map[String, AnyRef]] = {
    var cdata = List[Map[String, AnyRef]]()
    if (null != propertyMap && propertyMap.nonEmpty && propertyMap.contains("dialcodes")) {
      val dialcodeMap = propertyMap("dialcodes").asInstanceOf[Map[String, AnyRef]]
      val dialcodes = dialcodeMap("nv").asInstanceOf[List[String]]
      if (null != dialcodes) {
        var map = Map[String, AnyRef]()
        map ++= Map("id" -> dialcodes)
        map ++= Map("type" -> "DialCode")
        cdata :+= map
      }
    }
    if (null != addedRelations && addedRelations.nonEmpty) cdata ++= prepareCMap(addedRelations)
    if (null != removedRelations && removedRelations.nonEmpty) cdata ++= prepareCMap(removedRelations)
    cdata
  }

  /**
   * @param relations
   */
  private def prepareCMap(relations: List[Map[String, AnyRef]]): List[Map[String, AnyRef]] = {
    relations.map(relation => {
      var cMap = Map[String, AnyRef]()
      cMap ++= Map("id" -> relation("id"))
      cMap ++= Map("type" -> relation("type"))
      cMap
    })
  }

  /**
   *
   * @param relations
   */
  private def getRelationProps(relations: List[Map[String, AnyRef]], objectDefinition: ObjectDefinition)(implicit config: AuditEventGeneratorConfig):List[String] = {
    var relationProps = List[String]()
    if (relations.nonEmpty) {
      relations.foreach(rel => {
        val direction = rel.getOrElse("dir", "").asInstanceOf[String]
        val relationType = rel.getOrElse("rel", "").asInstanceOf[String]
        val targetObjType = rel.getOrElse("type", "").asInstanceOf[String]
        val relationProp = objectDefinition.relationLabel(targetObjType, direction, relationType)
        if (relationProp.nonEmpty) {
          relationProps :+= relationProp.get
        }
      })
    }
    relationProps
  }

  /**
   * @param oldDate
   * @param newDate
   * @return
   */
  def computeDuration(oldDate: String, newDate: String): Long = {
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    val od = sdf.parse(oldDate)
    val nd = sdf.parse(newDate)
    val diff = nd.getTime - od.getTime
    val diffSeconds = diff / 1000
    diffSeconds
  }
}