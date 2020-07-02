package org.sunbird.kp.course.functions

import java.lang.reflect.Type
import java.{lang, util}

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Batch, QueryBuilder, Select, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.sunbird.async.core.cache.{DataCache, RedisConnect}
import org.sunbird.async.core.job.{Metrics, WindowBaseProcessFunction}
import org.sunbird.async.core.util.CassandraUtil
import org.sunbird.kp.course.domain._
import org.sunbird.kp.course.task.CourseMetricsAggregatorConfig

import scala.collection.JavaConverters._
import scala.collection.mutable

class ProgressUpdater(config: CourseMetricsAggregatorConfig)(implicit val stringTypeInfo: TypeInformation[String],
                                                             @transient var cassandraUtil: CassandraUtil = null
) extends WindowBaseProcessFunction[util.Map[String, AnyRef], String, String](config) {
  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ProgressUpdater])
  private var cache: DataCache = _
  lazy private val gson = new Gson()
  val actionType = "batch-enrolment-update"

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.totalEventsCount, config.dbUpdateCount, config.cacheHitCount)
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

  override def process(key: String,
                       context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context,
                       events: lang.Iterable[util.Map[String, AnyRef]], metrics: Metrics): Unit = {
    events.forEach(event => {
      val batch: Batch = QueryBuilder.batch() // It holds all the batch of progress
      val eventData = event.get("edata").asInstanceOf[util.Map[String, AnyRef]]
      if (eventData.get("action") == actionType) { //TODO: Need to write other function to seperate the events based on action specific
        metrics.incCounter(config.totalEventsCount) // To Measure the number of batch-enrollment-updater events came.
        // PrimaryFields = <batchid, userid, courseid>
        val primaryFields = eventData.asScala.map(v => (v._1.toLowerCase, v._2)).filter(x => config.primaryFields.contains(x._1))
        // content-status object from the telemetry "BE_JOB_REQUEST"
        val csFromEvent = getContentStatusFromEvent(eventData)
        //To compute the unit level progress
        getUnitProgress(csFromEvent, primaryFields, context, metrics)
          .map(unit => batch.add(getQuery(unit._2, config.dbKeyspace, config.dbActivityAggTable)))
        // To compute the course progress
        getCourseProgress(csFromEvent, primaryFields, metrics)
          .map(course => batch.add(getQuery(course._2, config.dbKeyspace, config.dbActivityAggTable)))
        // To update the both unit and course progress into db
        writeToDb(batch.toString, metrics)
      } else {
        logger.debug("Invalid action type")
      }
    })
  }

  def getCourseProgress(csFromEvent: mutable.HashMap[String, Int], primaryFields: mutable.Map[String, AnyRef], metrics: Metrics): Map[String, Progress] = {
    val courseId = s"${primaryFields.get("courseid").orNull}"
    val leafNodes = readFromCache(key = s"$courseId:leafnodes", metrics)
    if (leafNodes.isEmpty) throw new Exception(s"LeafNodes are not available. courseId:$courseId") // Stop The job if the leafnodes are not available
    val courseContentsStatus = getContentStatusFromDB(primaryFields ++ Map(config.contentId -> leafNodes.asScala.toList))
    Map(courseId -> computeProgress(Map(config.activityType -> config.courseActivityType, config.activityUser -> primaryFields.get(config.userId).orNull, config.contextId -> s"cb:${primaryFields.get(config.batchId).orNull}", config.activityId -> s"${primaryFields.get(config.courseId).orNull}"), leafNodes, courseContentsStatus, csFromEvent))
  }

  def getUnitProgress(csFromEvent: mutable.HashMap[String, Int],
                      primaryFields: mutable.Map[String, AnyRef],
                      context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context,
                      metrics: Metrics
                     ): mutable.Map[String, Progress] = {
    val unitProgressMap = mutable.Map[String, Progress]()

    def progress(id: String): Progress = unitProgressMap.get(id).orNull

    val courseId = s"${primaryFields.get(config.courseId).orNull}"
    csFromEvent.map(contentId => {
      // Get the ancestors for the specific resource
      context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(primaryFields, generationFor = "content", contentId._2 == config.completedStatusCode, contentId._1))) // Get the Telemetry for resource type event
      val unitLevelAncestors = Option(readFromCache(key = s"$courseId:${contentId._1}:ancestors", metrics)).map(x => x.asScala.filter(_ > courseId)).getOrElse(List())
      unitLevelAncestors.map(unitId => {
        if (progress(unitId) == null) { // To avoid the computation iteration for unit
          // Get the leafNodes for the specific unit
          val unitLeafNodes: util.List[String] = Option(readFromCache(key = s"${unitId}:leafnodes", metrics)).getOrElse(new util.LinkedList())
          // Stop the job if the leaf-nodes are not available
          if (unitLeafNodes.isEmpty) throw new Exception(s"LeafNodes are not available. unitId:$unitId, courseId:$courseId")
          val cols = Map(config.activityType -> config.unitActivityType, config.activityUser -> primaryFields.get(config.userId).orNull, config.contextId -> s"cb:${primaryFields.get(config.batchId).orNull}", config.activityId -> s"$unitId")
          // Get all the content status for the leaf nodes of the particular unit
          val unitContentsStatusFromDB = getContentStatusFromDB(primaryFields ++ Map(config.contentId -> unitLeafNodes.asScala.toList))
          val progress = computeProgress(cols, unitLeafNodes, unitContentsStatusFromDB, csFromEvent)
          unitProgressMap += (unitId -> progress)
          context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(primaryFields, "course-unit", progress.isCompleted, unitId)))
        }
      })
    })
    unitProgressMap
  }

  def readFromDB(columns: mutable.Map[String, AnyRef], keySpace: String, table: String): List[Row]
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
                      leafNodes: util.List[String],
                      csFromDB: Map[String, Int],
                      csFromEvent: mutable.Map[String, Int]):
  Progress = {
    val unionKeys = csFromEvent.keySet.union(csFromDB.keySet)
    val mergedContentStatus: Map[String, Int] = unionKeys.map { key =>
      key -> (if (csFromEvent.getOrElse(key, 0) >= csFromDB.getOrElse(key, 0)) csFromEvent.getOrElse(key, 0)
      else csFromDB.getOrElse(key, 0))
    }.toMap.filter(value => value._2 == config.completedStatusCode).filter(requiredNodes => leafNodes.contains(requiredNodes._1))
    val agg = Map(config.progress -> mergedContentStatus.size) // It has only completed nodes id details
    val aggUpdatedOn = Map(config.progress -> new DateTime().getMillis) // Progress updated time
    val isCompleted: Boolean = mergedContentStatus.size == leafNodes.size()
    Progress(cols.get(config.activityType).orNull.asInstanceOf[String], cols.get(config.activityUser).orNull.asInstanceOf[String],
      cols.get(config.activityId).orNull.asInstanceOf[String], cols.get(config.contextId).orNull.asInstanceOf[String],
      agg, aggUpdatedOn, isCompleted = isCompleted)
  }


  /**
   * Method to get the content status object in map format ex: (do_5874308329084 -> 2, do_59485345435 -> 3)
   * It always takes the highest precedence progress values for the contents ex: (do_5874308329084 -> 2, do_5874308329084 -> 1, do_59485345435 -> 3) => (do_5874308329084 -> 2, do_59485345435 -> 3)
   *
   * @param eventData
   * @return
   */
  def getContentStatusFromEvent(eventData: util.Map[String, AnyRef]): mutable.HashMap[String, Int] = {
    val csFromEvent = new mutable.HashMap[String, Int]()
    val contentsList = eventData.get("contents").asInstanceOf[util.List[util.Map[String, AnyRef]]]
    contentsList.forEach(content => {
      if (csFromEvent.getOrElse(content.get("contentId").asInstanceOf[String], 0) < content.get("status").asInstanceOf[Double].toInt) {
        csFromEvent.put(content.get("contentId").asInstanceOf[String], content.get("status").asInstanceOf[Double].toInt)
      }
    })
    csFromEvent
  }

  def getContentStatusFromDB(primaryFields: mutable.Map[String, AnyRef]): Map[String, Int] = {
    Option(readFromDB(primaryFields, config.dbKeyspace, config.dbContentConsumptionTable))
      .toList.flatMap(list => list.map(res => mutable.Map(res.getObject(config.contentId) -> res.getObject("status")).asInstanceOf[mutable.Map[String, Int]])).flatten.toMap
  }

  def generateTelemetry(primaryFields: mutable.Map[String, AnyRef], generationFor: String, isCompleted: Boolean, activityId: String)
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
