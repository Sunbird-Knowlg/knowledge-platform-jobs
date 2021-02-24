package org.sunbird.job.service

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.domain.Event
import org.sunbird.job.task.AuditEventGeneratorConfig
import org.sunbird.job.util.JSONUtil
import org.ekstep.telemetry.TelemetryGenerator
import org.ekstep.telemetry.TelemetryParams

import java.util
import scala.collection.mutable
import java.text.SimpleDateFormat

class AuditEventGeneratorService(implicit config: AuditEventGeneratorConfig) {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditEventGeneratorService])
  private val IMAGE_SUFFIX = ".img"
  private val OBJECT_TYPE_IMAGE_SUFFIX = "Image"
  private val SKIP_AUDIT = "{\"object\": {\"type\":null}}"

  private val systemPropsList = List("IL_SYS_NODE_TYPE", "IL_FUNC_OBJECT_TYPE", "IL_UNIQUE_ID", "IL_TAG_NAME", "IL_ATTRIBUTE_NAME", "IL_INDEXABLE_METADATA_KEY", "IL_NON_INDEXABLE_METADATA_KEY",
    "IL_IN_RELATIONS_KEY", "IL_OUT_RELATIONS_KEY", "IL_REQUIRED_PROPERTIES", "IL_SYSTEM_TAGS_KEY", "IL_SEQUENCE_INDEX","SYS_INTERNAL_LAST_UPDATED_ON", "lastUpdatedOn", "versionKey", "lastStatusChangedOn")

  private def getContext(channelId: String, env: String): Map[String, String] = {
    val context = Map(
      TelemetryParams.ACTOR.name -> "org.ekstep.learning.platform",
      TelemetryParams.CHANNEL.name -> channelId,
      TelemetryParams.ENV.name -> env
    )
    context
  }

  /**
   * @param rDef
   * @param relDefMap
   * @param rel
   */
  private def getRelationDefinitionKey(rDef: Map[String, AnyRef], relDefMap: mutable.Map[String, String], rel: String): Unit = {
    if (null != rDef.get("objectTypes") && !rDef.get("objectTypes").isEmpty) {
      rDef.get("objectTypes").asInstanceOf[List[String]].map(objectType => {
        val key = rDef.get("relationName") + objectType + rel
        relDefMap ++= Map(key -> rDef.get("title").asInstanceOf[String])
      })
    }
  }

  def processEvent(message: util.Map[String, AnyRef], context: ProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]]#Context, metrics: Metrics): Unit = {
    logger.info("AUDIT Event::" + JSONUtil.serialize(message))
    logger.info("Input Message Received for : [" + message.get("nodeUniqueId") + "], Txn Event createdOn:" + message.get("createdOn") + ", Operation Type:" + message.get("operationType"))
    try {
      val auditMap = getAuditMessage(JSONUtil.deserialize[Map[String, AnyRef]](JSONUtil.serialize(message)))
      val objectType = auditMap.get("object").asInstanceOf[Map[String, AnyRef]].getOrElse("type", "").asInstanceOf[String]
      if (null != objectType && objectType != "") {
        logger.error("Failed to process message :: " + JSONUtil.serialize(auditMap))
        context.output(config.auditOutputTag, auditMap)
        logger.info("Telemetry Audit Message Successfully Sent for : " + auditMap.get("object").asInstanceOf[Map[String, AnyRef]].getOrElse("id", "").asInstanceOf[String] + " :: mid ::" + auditMap.get("mid"))
        metrics.incCounter(config.successEventCount)
      }
      else {
        logger.info("Skipped event as the objectype is not available, event =" + auditMap)
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case e: Exception =>

        logger.error("Failed to process message :: " + JSONUtil.serialize(message), e)
        metrics.incCounter(config.failedEventCount)
    }
  }

  def getDefinition(graphId: String, objectType: String): Map[String, AnyRef] = {
    Map[String, AnyRef]()
  }


  def getAuditMessage(message: Map[String, AnyRef]): util.Map[String, AnyRef] = {
    var auditMap:util.Map[String, AnyRef] = null
    var objectId = message.getOrElse("nodeUniqueId", null).asInstanceOf[String]
    var objectType = message.getOrElse("objectType", null).asInstanceOf[String]
    val env = if (null != objectType) objectType.toLowerCase.replace("image", "") else "system"
    val graphId = message("graphId").asInstanceOf[String]
    val userId = message("userId").asInstanceOf[String]
    val definitionNode:Map[String, AnyRef] = getDefinition(graphId, objectType)
    val inRelations = mutable.Map[String, String]()
    val outRelations = mutable.Map[String, String]()
    getRelationDefinitionMaps(definitionNode, inRelations, outRelations)

    var channelId = config.defaultChannel
    val channel = message.getOrElse("channel", "").asInstanceOf[String]
    if (null != channel) channelId = channel

    val transactionData = message("transactionData").asInstanceOf[Map[String, AnyRef]]
    val propertyMap = transactionData("properties").asInstanceOf[Map[String, AnyRef]]
    val statusMap = propertyMap.getOrElse("status", null).asInstanceOf[Map[String, AnyRef]]
    val lastStatusChangedOn = propertyMap.getOrElse("lastStatusChangedOn", null).asInstanceOf[Map[String, AnyRef]]
    val addedRelations = transactionData.getOrElse("addedRelations", null).asInstanceOf[List[Map[String, AnyRef]]]
    val removedRelations = transactionData.getOrElse("removedRelations", null).asInstanceOf[List[Map[String, AnyRef]]]
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
    var props:List[String] = propertyMap.keys.toList
    props ++= getRelationProps(addedRelations, inRelations, outRelations)
    props ++= getRelationProps(removedRelations, inRelations, outRelations)
    val propsExceptSystemProps = props.filter(prop => !systemPropsList.contains(prop))
    val cdata = getCData(addedRelations, removedRelations, propertyMap)
    var context:Map[String, String] = getContext(channelId, env)
    objectId = if (null != objectId) objectId.replaceAll(IMAGE_SUFFIX, "")
    else objectId
    objectType = if (null != objectType) objectType.replaceAll(OBJECT_TYPE_IMAGE_SUFFIX, "")
    else objectType
    context ++= Map("objectId" -> objectId)
    context ++= Map("objectType" -> objectType)
    if (StringUtils.isNotBlank(duration)) context ++= Map("duration" -> duration)
    if (StringUtils.isNotBlank(pkgVersion)) context ++= Map("pkgVersion" -> pkgVersion)
    if (StringUtils.isNotBlank(userId)) context ++= Map(TelemetryParams.ACTOR.name -> userId)
    if (propsExceptSystemProps.nonEmpty) {
      val auditMessage = TelemetryGenerator.audit(
        JSONUtil.deserialize[util.Map[String, String]](JSONUtil.serialize(context)),
        JSONUtil.deserialize[util.List[String]](JSONUtil.serialize(propsExceptSystemProps)),
        currStatus,
        prevStatus,
        JSONUtil.deserialize[util.List[util.Map[String, AnyRef]]](JSONUtil.serialize(cdata)))
      logger.info("Audit Message for Content Id [" + objectId + "] : " + auditMessage);
      auditMap = JSONUtil.deserialize[util.Map[String, AnyRef]](auditMessage)
    }
    else {
      logger.info("Skipping Audit log as props is null or empty")
      auditMap = JSONUtil.deserialize[util.Map[String, AnyRef]](SKIP_AUDIT)
    }
    auditMap
  }

  /**
   * @param addedRelations
   * @param removedRelations
   * @param propertyMap
   * @return
   */
  private def getCData(addedRelations: List[Map[String, AnyRef]], removedRelations: List[Map[String, AnyRef]], propertyMap: Map[String, AnyRef]):List[Map[String, AnyRef]] = {
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
   * @param inRelations
   * @param outRelations
   * @return
   */
  private def getRelationProps(relations: List[Map[String, AnyRef]], inRelations: mutable.Map[String, String], outRelations: mutable.Map[String, String]) = {
    var props = List[String]()
    if (null != relations && !relations.isEmpty) {
      for (relation <- relations) {
        val key = relation("rel").asInstanceOf[String] + relation("type").asInstanceOf[String]
        if (StringUtils.equalsIgnoreCase(relation("dir").asInstanceOf[String], "IN")) {
          if (null != inRelations.getOrElse(key + "in", null)) props :+= inRelations(key + "in")
        } else if (null != outRelations.getOrElse(key + "out", null)) props :+= outRelations(key + "out")
      }
    }
    props
  }

  /**
   * @param definition
   * @param inRelations
   * @param outRelations
   */
  private def getRelationDefinitionMaps(definition: Map[String, AnyRef], inRelations: mutable.Map[String, String], outRelations: mutable.Map[String, String]): Unit = {
    if (null != definition) {
      if (null != definition.get("inRelations") && !definition.get("inRelations").isEmpty) {
        for (rDef <- definition.get("inRelations")) {
          getRelationDefinitionKey(rDef.asInstanceOf[Map[String, AnyRef]], inRelations, "in")
        }
      }
      if (null != definition.get("outRelations") && !definition.get("outRelations").isEmpty) {
        for (rDef <- definition.get("outRelations")) {
          getRelationDefinitionKey(rDef.asInstanceOf[Map[String, AnyRef]], outRelations, "out")
        }
      }
    }
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