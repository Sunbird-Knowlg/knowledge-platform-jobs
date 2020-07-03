package org.sunbird.kp.course.functions

import java.lang.reflect.Type
import java.util

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Batch, QueryBuilder, Select, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.sunbird.async.core.cache.{DataCache, RedisConnect}
import org.sunbird.async.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.async.core.util.CassandraUtil
import org.sunbird.kp.course.domain._
import org.sunbird.kp.course.task.CourseMetricsAggregatorConfig

import scala.collection.JavaConverters._

class ProgressUpdater(config: CourseMetricsAggregatorConfig)(implicit val stringTypeInfo: TypeInformation[String],
                                                             @transient var cassandraUtil: CassandraUtil = null
) extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {
  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ProgressUpdater])
  private var cache: DataCache = _
  lazy private val gson = new Gson()

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.dbUpdateCount, config.dbReadCount, config.cacheHitCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
    cache.init()
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: util.Map[String, AnyRef], context: ProcessFunction[util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val batch: Batch = QueryBuilder.batch() // It holds all the batch of progress
    val eventData: Map[String, AnyRef] = event.get("edata").asInstanceOf[util.Map[String, AnyRef]].asScala.toMap
    // PrimaryFields = <batchid, userid, courseid>
    val primaryFields: Map[String, AnyRef] = eventData.map(v => (v._1.toLowerCase, v._2)).filter(x => config.primaryFields.contains(x._1))
    // contents object from the telemetry "BE_JOB_REQUEST"
    val contents: List[Map[String, AnyRef]] = eventData.getOrElse("contents", new util.ArrayList[Map[String, AnyRef]]()).asInstanceOf[util.ArrayList[Map[String, AnyRef]]].asScala.toList
    // Create a content status map using contents list and remove the lowest precedence stats if have any duplicate content
    val csFromEvent = getContentStatusFromEvent(contents)
    //To compute the progress to units
    getUnitProgress(csFromEvent, primaryFields, context, metrics)
      .map(unit => batch.add(getQuery(unit._2, config.dbKeyspace, config.dbActivityAggTable)))
    // To compute the course progress
    getCourseProgress(csFromEvent, primaryFields, metrics)
      .map(course => batch.add(getQuery(course._2, config.dbKeyspace, config.dbActivityAggTable)))
    // To update the both unit and course progress into db
    writeToDb(batch.toString, metrics)
  }

  def getCourseProgress(csFromEvent: Map[String, Number], primaryFields: Map[String, AnyRef], metrics: Metrics): Map[String, Progress] = {
    val courseId = s"${primaryFields.get("courseid").orNull}"
    val leafNodes = readFromCache(key = s"$courseId:${config.leafNodes}", metrics).asScala.toList
    if (leafNodes.isEmpty) throw new Exception(s"LeafNodes are not available. courseId:$courseId") // Stop The job if the leafnodes are not available
    val courseContentsStatus: Map[String, Number] = getContentStatusFromDB(primaryFields ++ Map(config.contentId -> leafNodes), metrics)
    Map(courseId -> computeProgress(Map(config.activityType -> config.courseActivityType, config.activityUser -> primaryFields.get(config.userId).orNull, config.contextId -> s"cb:${primaryFields.get(config.batchId).orNull}", config.activityId -> s"${primaryFields.get(config.courseId).orNull}"), leafNodes, courseContentsStatus, csFromEvent))
  }

  def getUnitProgress(csFromEvent: Map[String, Number],
                      primaryFields: Map[String, AnyRef],
                      context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
                      metrics: Metrics
                     ): Map[String, Progress] = {
    val courseId = s"${primaryFields.get(config.courseId).orNull}"

    // Get the ancestors for the specific resource
    val contentAncestors: List[String] = csFromEvent.map(contentId => {
      context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(primaryFields, generationFor = "content", contentId._2.intValue() == config.completedStatusCode, contentId._1))) // Get the Telemetry for resource type event
      contentId._1 -> readFromCache(key = s"$courseId:${contentId._1}:${config.ancestors}", metrics).asScala.filter(_ > courseId).toList.distinct
    }).values.flatten.toList.distinct

    // Get the leafNodes for the unit from the redis cache
    val unitLeafNodes: Map[String, List[String]] = contentAncestors.map(unitId => {
      val leafNodes: List[String] = readFromCache(key = s"${unitId}:${config.leafNodes}", metrics).asScala.toList.distinct
      if (leafNodes.isEmpty) throw new Exception(s"Leaf nodes are not available for this unitId:$unitId and courseId:$courseId")
      (unitId -> leafNodes.distinct)
    }).toMap

    // Get the progress for each unit node
    unitLeafNodes.map(unitLeafMap => {
      val cols = Map(config.activityType -> config.unitActivityType, config.activityUser -> primaryFields.get(config.userId).orNull, config.contextId -> s"cb:${primaryFields.get(config.batchId).orNull}", config.activityId -> s"${unitLeafMap._1}")
      val contentStatus = getContentStatusFromDB(primaryFields ++ Map(config.contentId -> unitLeafMap._2), metrics)
      val progress = computeProgress(cols, unitLeafMap._2, contentStatus, csFromEvent)
      logger.info(s"Unit: ${unitLeafMap._1} completion status: ${progress.isCompleted}")
      context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(primaryFields, "course-unit", progress.isCompleted, unitLeafMap._1)))
      (unitLeafMap._1 -> progress)
    })
  }

  def readFromDB(columns: Map[String, AnyRef], keySpace: String, table: String, metrics: Metrics): List[Row]
  = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(keySpace, table).
      where()
    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.find(selectWhere.toString).asScala.toList

  }

  def writeToDb(query: String, metrics: Metrics): Unit
  = {
    cassandraUtil.upsert(query)
    metrics.incCounter(config.successEventCount)
    metrics.incCounter(config.dbUpdateCount)
  }

  def readFromCache(key: String, metrics: Metrics): util.List[String] = {
    metrics.incCounter(config.cacheHitCount)
    cache.lRangeWithRetry(key)
  }

  def getQuery(progress: Progress, keySpace: String, table: String):
  Update.Where = {
    QueryBuilder.update(keySpace, table)
      .`with`(QueryBuilder.putAll(config.agg, progress.agg.asJava))
      .and(QueryBuilder.putAll(config.aggLastUpdated, progress.agg_last_updated.asJava))
      .where(QueryBuilder.eq(config.activityId, progress.activity_id))
      .and(QueryBuilder.eq(config.activityType, progress.activity_type))
      .and(QueryBuilder.eq(config.contextId, progress.context_id))
      .and(QueryBuilder.eq(config.activityUser, progress.user_id))
  }


  /**
   * Method to compute the progress by comparing the content status
   *
   * @param cols        unit agg table columns list
   * @param leafNodes   - number of leafNodes either
   * @param csFromDB    - content status from database  Map(do_54395 -> 1, do_u5oi957438 -> 2)
   * @param csFromEvent - content status from the event ex: Map(do_54395 -> 2)
   * @return Progress - It will returns the computed progress
   */

  def computeProgress(cols: Map[String, AnyRef],
                      leafNodes: List[String],
                      csFromDB: Map[String, Number],
                      csFromEvent: Map[String, Number]):
  Progress = {
    val unionKeys = csFromEvent.keySet.union(csFromDB.keySet)
    val mergedContentStatus: Map[String, Int] = unionKeys.map { key =>
      key -> (if (csFromEvent.getOrElse(key, 0).asInstanceOf[Number].intValue() >= csFromDB.getOrElse(key, 0).asInstanceOf[Number].intValue()) csFromEvent.getOrElse(key, 0).asInstanceOf[Number].intValue()
      else csFromDB.getOrElse(key, 0).asInstanceOf[Number].intValue())
    }.toMap.filter(value => value._2 == config.completedStatusCode).filter(requiredNodes => leafNodes.contains(requiredNodes._1))
    val agg = Map(config.progress -> mergedContentStatus.size) // It has only completed nodes id details
    val aggUpdatedOn = Map(config.progress -> new DateTime().getMillis) // Progress updated time
    val isCompleted: Boolean = mergedContentStatus.size == leafNodes.size
    Progress(cols.get(config.activityType).orNull.asInstanceOf[String], cols.get(config.activityUser).orNull.asInstanceOf[String],
      cols.get(config.activityId).orNull.asInstanceOf[String], cols.get(config.contextId).orNull.asInstanceOf[String],
      agg, aggUpdatedOn, isCompleted = isCompleted)
  }


  /**
   * Method to get the content status object in map format ex: (do_5874308329084 -> 2, do_59485345435 -> 3)
   * It always takes the highest precedence progress values for the contents ex: (do_5874308329084 -> 2, do_5874308329084 -> 1, do_59485345435 -> 3) => (do_5874308329084 -> 2, do_59485345435 -> 3)
   *
   * @return
   */
  def getContentStatusFromEvent(contents: List[Map[String, AnyRef]]): Map[String, Number] = {
    contents.asInstanceOf[List[util.Map[String, AnyRef]]].map(content => {
      (content.asScala.toMap.get("contentId").orNull.asInstanceOf[String], content.asScala.toMap.get("status").orNull.asInstanceOf[Number])
    }).groupBy(id => id._1.asInstanceOf[String])
      .map(status => status._2.maxBy(_._1))
  }

  def getContentStatusFromDB(primaryFields: Map[String, AnyRef], metrics: Metrics): Map[String, Number] = {
    Option(readFromDB(primaryFields, config.dbKeyspace, config.dbContentConsumptionTable, metrics))
      .toList.flatMap(list => list.map(res => Map(res.getObject(config.contentId) -> res.getObject("status")).asInstanceOf[Map[String, Number]])).flatten.toMap
  }

  def generateTelemetry(primaryFields: Map[String, AnyRef], generationFor: String, isCompleted: Boolean, activityId: String)
  : TelemetryEvent = {
    generationFor.toUpperCase() match {
      case "COURSE-UNIT" =>
        TelemetryEvent(actor = ActorObject(id = primaryFields.get("courseid").orNull.asInstanceOf[String]), edata = if (isCompleted) EventData(props = Array(new DateTime().getMillis.toString), `type` = "completed") else EventData(props = Array(activityId), `type` = "start"),
          context = EventContext(cdata = Array(Map("type" -> "CourseBatch", "id" -> primaryFields.get(config.batchId).orNull).asJava)),
          `object` = EventObject(rollup = Map("l1" -> primaryFields.get(config.courseId).orNull.asInstanceOf[String]).asJava, id = activityId, `type` = "CourseUnit")
        )
      case "CONTENT" =>
        TelemetryEvent(
          actor = ActorObject(id = primaryFields.get(config.userId).orNull.asInstanceOf[String]),
          edata = if (isCompleted) EventData(props = Array(new DateTime().getMillis.toString), `type` = "completed") else EventData(props = Array(activityId), `type` = "start"),
          context = EventContext(cdata = Array(Map("type" -> "CourseBatch", "id" -> primaryFields.get(config.batchId).orNull).asJava)),
          `object` = EventObject(rollup = Map("l1" -> primaryFields.get(config.courseId).orNull.asInstanceOf[String], "l2" -> primaryFields.get(config.batchId).orNull.asInstanceOf[String]).asJava, id = activityId, `type` = "Content")
        )
      case "COURSE" => logger.debug("Telemetry is not generated")
        null
    }

  }


}
