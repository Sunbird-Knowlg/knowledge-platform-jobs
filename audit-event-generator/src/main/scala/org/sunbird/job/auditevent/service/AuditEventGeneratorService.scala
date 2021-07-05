package org.sunbird.job.auditevent.service

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.auditevent.task.AuditEventGeneratorConfig
import org.sunbird.job.util.JSONUtil
import org.sunbird.telemetry.TelemetryGenerator
import org.sunbird.telemetry.TelemetryParams
import org.sunbird.job.auditevent.domain.Event
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import com.google.gson.Gson
import org.sunbird.job.exception.InvalidEventException

import java.util
import java.text.SimpleDateFormat

trait AuditEventGeneratorService {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditEventGeneratorService])
  private val OBJECT_TYPE_IMAGE_SUFFIX = "Image"
  private val SKIP_AUDIT = """{"object": {"type":null}}"""
  private lazy val definitionCache = new DefinitionCache
  private lazy val gson = new Gson

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

  @throws(classOf[InvalidEventException])
  def processEvent(message: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics)(implicit config: AuditEventGeneratorConfig): Unit = {
    logger.info("AUDIT Event::" + JSONUtil.serialize(message))
    logger.info("Input Message Received for : [" + message.nodeUniqueId + "], Txn Event createdOn:" + message.createdOn + ", Operation Type:" + message.operationType)
    try {
      val (auditEventStr, objectType) = getAuditMessage(message)(config, metrics)
      if (StringUtils.isNotBlank(objectType)) {
        context.output(config.auditOutputTag, auditEventStr)
        logger.info("Telemetry Audit Message Successfully Sent for : " + message.objectId + " :: event ::" + auditEventStr)
        metrics.incCounter(config.successEventCount)
      }
      else {
        logger.info("Skipped event as the objectype is not available, event =" + auditEventStr)
        metrics.incCounter(config.emptyPropsEventCount)
      }
    } catch {
      case e: Exception =>
        logger.error("Failed to process message :: " + JSONUtil.serialize(message), e)
        throw e
    }
  }

  def getDefinition(objectType: String)(implicit config: AuditEventGeneratorConfig, metrics: Metrics): ObjectDefinition = {
    try {
      definitionCache.getDefinition(objectType, config.configVersion, config.basePath)
    } catch {
      case ex: Exception => {
        metrics.incCounter(config.emptySchemaEventCount)
        new ObjectDefinition(objectType, config.configVersion, Map[String, AnyRef](), Map[String, AnyRef]())
      }
    }
  }


  def getAuditMessage(message: Event)(implicit config: AuditEventGeneratorConfig, metrics: Metrics): (String, String) = {
    var auditMap: String = null
    var objectType = message.objectType
    val env = if (null != objectType) objectType.toLowerCase.replace("image", "") else "system"

    val definitionNode: ObjectDefinition = getDefinition(objectType)

    val propertyMap = message.transactionData("properties").asInstanceOf[Map[String, AnyRef]]
    val statusMap = propertyMap.getOrElse("status", null).asInstanceOf[Map[String, AnyRef]]
    val lastStatusChangedOn = propertyMap.getOrElse("lastStatusChangedOn", null).asInstanceOf[Map[String, AnyRef]]
    val addedRelations = message.transactionData.getOrElse("addedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
    val removedRelations = message.transactionData.getOrElse("removedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]

    var pkgVersion = ""
    var prevStatus = ""
    var currStatus = ""
    var duration = ""
    val pkgVerMap = propertyMap.getOrElse("pkgVersion", null).asInstanceOf[Map[String, AnyRef]]
    if (null != pkgVerMap) pkgVersion = s"${pkgVerMap.get("nv")}"

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

    var context: Map[String, String] = getContext(message.channelId(config.defaultChannel), env)

    objectType = if (null != objectType) objectType.replaceAll(OBJECT_TYPE_IMAGE_SUFFIX, "") else objectType
    context ++= Map("objectId" -> message.objectId, "objectType" -> objectType)

    if (StringUtils.isNotBlank(duration)) context ++= Map("duration" -> duration)
    if (StringUtils.isNotBlank(pkgVersion)) context ++= Map("pkgVersion" -> pkgVersion)
    if (StringUtils.isNotBlank(message.userId)) context ++= Map(TelemetryParams.ACTOR.name -> message.userId)

    if (propsExceptSystemProps.nonEmpty) {
      val cdataList = gson.fromJson(JSONUtil.serialize(cdata), classOf[java.util.List[java.util.Map[String, Object]]])

      TelemetryGenerator.setComponent("audit-event-generator")
      auditMap = TelemetryGenerator.audit(
        JSONUtil.deserialize[util.Map[String, String]](JSONUtil.serialize(context)),
        JSONUtil.deserialize[util.List[String]](JSONUtil.serialize(propsExceptSystemProps)),
        currStatus,
        prevStatus,
        cdataList)
      logger.info("Audit Message for Content Id [" + message.objectId + "] : " + auditMap);

      (auditMap, message.objectType)
    }
    else {
      logger.info("Skipping Audit log as props is null or empty")
      (SKIP_AUDIT, "")
    }
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
        cdata :+= Map[String, AnyRef]("id" -> dialcodes, "type" -> "DialCode")
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
      Map[String, AnyRef]("id" -> relation("id"), "type" -> relation("type"))
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