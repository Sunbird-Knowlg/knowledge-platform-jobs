package org.sunbird.async.functions

import java.lang.reflect.Type
import java.{lang, util}

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
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
import org.sunbird.async.domain._
import org.sunbird.async.task.CourseAggregateUpdaterConfig

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ProgressUpdater(config: CourseAggregateUpdaterConfig)(implicit val stringTypeInfo: TypeInformation[String], @transient var cassandraUtil: CassandraUtil = null) extends WindowBaseProcessFunction[util.Map[String, AnyRef], String, String](config) {
  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ProgressUpdater])
  private var cache: DataCache = _
  lazy private val gson = new Gson()
  var batch = new ListBuffer[Update.Where]()

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.batchEnrolmentUpdateEventCount, config.dbUpdateCount, config.dbReadCount, config.cacheHitCount)
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

  def process(key: String, context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context, events: lang.Iterable[util.Map[String, AnyRef]], metrics: Metrics): Unit = {

    val batchEnrolmentEvents = events.asScala.filter(event => event.get("edata").asInstanceOf[util.Map[String, AnyRef]].asScala("action").asInstanceOf[String] == "batch-enrolment-update")
    val eDataBatch = batchEnrolmentEvents.map(f => {
      metrics.incCounter(config.batchEnrolmentUpdateEventCount)
      (f.get("edata").asInstanceOf[util.Map[String, AnyRef]].asScala.toMap)
    }).to[ListBuffer]
    // Content status from the list of the events
    val csFromEvents: List[Map[String, AnyRef]] = eDataBatch.map(ed => ed ++ Map("contentStatus" -> getContentStatusFromEvent(ed("contents").asInstanceOf[util.List[Map[String, AnyRef]]].asScala.toList))).toList

    // Content status from the cassandra db for all batchId, UserId & courseId in the list of events
    val csFromDb = new ListBuffer[Map[String, AnyRef]]()
    eDataBatch.foreach(edata => {
      if (eDataBatch.nonEmpty) {
        val eDataSubBatch = eDataBatch.slice(0, config.maxQueryReadBatchSize) // Max read query
        csFromDb.appendAll(getContentStatusFromDB(Map("userid" -> eDataSubBatch.map(x => x("userId")).toList, "batchid" -> eDataSubBatch.map(x => x("batchId")).toList, "courseid" -> eDataSubBatch.map(x => x("courseId")).toList), metrics))
        eDataBatch.remove(0, eDataSubBatch.size)
      }
    })

    // CourseProgress
    csFromEvents.map(csFromEvent => getCourseProgress(csFromEvent, csFromDb.toList, metrics)
      .map(course => batch += getQuery(course._2, config.dbKeyspace, config.dbUserActivityAggTable)))

    // Unit Progress
    csFromEvents.map(csFromEvent => getUnitProgress(csFromEvent, csFromDb.toList, context, metrics)
      .map(course => batch += getQuery(course._2, config.dbKeyspace, config.dbUserActivityAggTable)))

    // Update batch of queries into cassandra
    createBatchAndUpdateDB(config.maxQueryWriteBatchSize, metrics)
  }


  def getCourseProgress(csFromEvent: Map[String, AnyRef], csFromDb: List[Map[String, AnyRef]], metrics: Metrics): Map[String, Progress] = {
    val courseId = csFromEvent.getOrElse("courseId", null).asInstanceOf[String]
    val batchId = csFromEvent.getOrElse("batchId", null).asInstanceOf[String]
    val userId = csFromEvent.getOrElse("userId", null).asInstanceOf[String]
    val leafNodes = readFromCache(key = s"$courseId:${config.leafNodes}", metrics).asScala.toList
    if (leafNodes.isEmpty) {
      metrics.incCounter(config.failedEventCount)
      throw new Exception(s"LeafNodes are not available. courseId:$courseId")
    }
    val courseContentsStatusFromDb = csFromDb.filter(cs => cs("courseId").asInstanceOf[String] != courseId && cs("batchId").asInstanceOf[String] != batchId && cs("userId").asInstanceOf[String] != userId)
    Map(courseId -> computeProgress(Map(config.activityType -> config.courseActivityType, config.activityUser -> userId,
      config.contextId -> s"cb:$batchId", config.activityId -> courseId), leafNodes,
      courseContentsStatusFromDb.head, csFromEvent("contentStatus").asInstanceOf[Map[String, AnyRef]]))
  }

  def getUnitProgress(csFromEvent: Map[String, AnyRef],
                      csFromDb: List[Map[String, AnyRef]],
                      context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context,
                      metrics: Metrics
                     ): Map[String, Progress] = {
    val courseId = csFromEvent.getOrElse("courseId", null).asInstanceOf[String]
    val batchId = csFromEvent.getOrElse("batchId", null).asInstanceOf[String]
    val userId = csFromEvent.getOrElse("userId", null).asInstanceOf[String]
    val contentStatus: Map[String, AnyRef] = csFromEvent.getOrElse("contentStatus", null).asInstanceOf[Map[String, AnyRef]]
    // Get the ancestors for the specific resource
    val contentAncestors: List[String] = contentStatus.map(contentId => {
      context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(csFromEvent, generationFor = "content", contentId._2.asInstanceOf[Number].intValue() == config.completedStatusCode, contentId._1))) // Get the Telemetry for resource type event
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
      val cols = Map(config.activityType -> config.unitActivityType, config.activityUser -> userId, config.contextId -> s"cb:$batchId", config.activityId -> s"${unitLeafMap._1}")
      val courseContentsStatusFromDb = csFromDb.filter(cs => cs("courseId").asInstanceOf[String] != courseId && cs("batchId").asInstanceOf[String] != batchId && cs("userId").asInstanceOf[String] != userId)
      val progress = computeProgress(cols, unitLeafMap._2, courseContentsStatusFromDb.head, contentStatus)
      logger.info(s"Unit: ${unitLeafMap._1} completion status: ${progress.isCompleted}")
      context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(csFromEvent, "course-unit", progress.isCompleted, unitLeafMap._1)))
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

  def updateDB(batch: List[Update.Where], metrics: Metrics): Boolean
  = {
    val cqlBatch = QueryBuilder.batch()
    batch.map(x => cqlBatch.add(x))
    val status = cassandraUtil.upsert(cqlBatch.toString)
    metrics.incCounter(config.successEventCount)
    metrics.incCounter(config.dbUpdateCount)
    status
  }

  def createBatchAndUpdateDB(batchSize: Int, metrics: Metrics): Unit = {
    batch.foreach(query => {
      if (batch.nonEmpty) {
        val totalElements = batch.slice(0, batchSize).toList
        val updateStatus = updateDB(totalElements, metrics)
        if (updateStatus) {
          batch.remove(0, totalElements.size)
        } else {
          logger.error("Database update has failed")
        }
      }
    })
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
                      csFromDB: Map[String, AnyRef],
                      csFromEvent: Map[String, AnyRef]):
  Progress = {
    val contentStatusFromDB = csFromDB.getOrElse("contentStatus", Map()).asInstanceOf[Map[String, Number]]
    val unionKeys = csFromEvent.keySet.union(csFromDB.keySet)
    val mergedContentStatus: Map[String, Int] = unionKeys.map { key =>
      key -> (if (csFromEvent.getOrElse(key, 0).asInstanceOf[Number].intValue() >= contentStatusFromDB.getOrElse(key, 0).asInstanceOf[Number].intValue()) csFromEvent.getOrElse(key, 0).asInstanceOf[Number].intValue()
      else contentStatusFromDB.getOrElse(key, 0).asInstanceOf[Number].intValue())
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

  def getContentStatusFromDB(primaryFields: Map[String, AnyRef], metrics: Metrics): List[Map[String, AnyRef]] = {
    val records = Option(readFromDB(primaryFields, config.dbKeyspace, config.dbContentConsumptionTable, metrics))
    records.map(record => record.groupBy(col => Map("batchId" -> col.getObject("batchid").asInstanceOf[String], "userId" -> col.getObject("userid").asInstanceOf[String], "courseId" -> col.getObject("courseid").asInstanceOf[String])))
      .map(groupedRecords => groupedRecords.map(groupedRecord => Map(groupedRecord._1.asInstanceOf[Map[String, String]]
        -> groupedRecord._2.flatMap(mapRec => Map(mapRec.getObject("contentid").asInstanceOf[String] -> mapRec.getObject("status"))).toMap))).toList.flatten
      .flatMap(d => d.keySet.map(z => Map("userId" -> z("userId"), "courseId" -> z("courseId"), "batchId" -> z("batchId"), "contentStatus" -> d.values.flatten.toMap)).toList)
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
