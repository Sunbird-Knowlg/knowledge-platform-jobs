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
import org.sunbird.kp.course.domain.{Progress, TelemetryEvent}
import org.sunbird.kp.course.task.CourseAggregatorConfig

import scala.collection.JavaConverters._
import scala.collection.mutable

class ProgressUpdater(config: CourseAggregatorConfig)(implicit val stringTypeInfo: TypeInformation[String],
                                                      @transient var cassandraUtil: CassandraUtil = null
) extends WindowBaseProcessFunction[util.Map[String, AnyRef], String, String](config) {
  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ProgressUpdater])
  private var redisCache: DataCache = _
  private var ancestorsCache: DataCache = _
  lazy private val gson = new Gson()
  val actionType = "batch-enrolment-update"

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.totalEventsCount, config.dbUpdateCount, config.cacheHitCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    redisCache = new DataCache(config, new RedisConnect(config), config.leafNodesStore, List())
    redisCache.init()
  }

  override def close(): Unit = {
    super.close()
  }

  override def process(key: String,
                       context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context,
                       events: lang.Iterable[util.Map[String, AnyRef]], metrics: Metrics): Unit = {

    val batch: Batch = QueryBuilder.batch()
    events.forEach(event => {
      val eventData = event.get("edata").asInstanceOf[util.Map[String, AnyRef]]
      if (eventData.get("action") == actionType) {
        metrics.incCounter(config.totalEventsCount) // To Measure the number of batch-enrollment-updater events came.
        val primaryFields = eventData.asScala.map(v => (v._1.toLowerCase, v._2)).filter(x => config.primaryFields.contains(x._1))
        val csFromEvent = getContentStatusFromEvent(eventData)
        getUnitProgress(csFromEvent, primaryFields, metrics)
          .map(unit => batch.add(getQuery(unit._2, config.dbKeyspace, "activity_agg")))
        getCourseProgress(csFromEvent, primaryFields, metrics)
          .map(course => batch.add(getQuery(course._2, config.dbKeyspace, "activity_agg")))
        println("BatchQueryIs" + batch.toString)
        writeToDb(batch.toString, metrics)
      }
    })
  }

  def getUnitProgress(csFromEvent: mutable.HashMap[String, Int], primaryFields: mutable.Map[String, AnyRef], metrics: Metrics): mutable.Map[String, Progress] = {
    val unitProgressMap = mutable.Map[String, Progress]()

    def progress(id: String): Progress = unitProgressMap.get(id).getOrElse(null)

    val courseId = s"${primaryFields.get("courseid").getOrElse(null)}"
    csFromEvent.map(contentId => {
      val unitLevelAncestors = Option(getDataFromRedis(s"$courseId:${contentId._1}:ancestors", metrics)).map(x => x.asScala.filter(_ > courseId)).getOrElse(List())
      unitLevelAncestors.map(unitId => {
        if (progress(unitId) == null) {
          val unitLeafNodes: util.List[String] = Option(getDataFromRedis(s"${unitId}:leafnodes", metrics)).getOrElse(new util.LinkedList())
          val cols = Map(config.activityType -> "course-unit", config.contextId -> s"cb:${primaryFields.get(config.batchId)}", config.activityId -> s"${unitId}")
          val unitContentsStatusFromDB = getContentStatusFromDB(primaryFields ++ Map(config.contentId -> unitLeafNodes.asScala.toList))
          unitProgressMap += (unitId -> computeProgress(cols, unitLeafNodes, unitContentsStatusFromDB, csFromEvent))
        }
      })
    })
    unitProgressMap
  }

  def getCourseProgress(csFromEvent: mutable.HashMap[String, Int], primaryFields: mutable.Map[String, AnyRef], metrics: Metrics): Map[String, Progress] = {
    val courseId = s"${primaryFields.get("courseid").getOrElse(null)}"
    val leafNodes = getDataFromRedis(key = s"$courseId:leafnodes", metrics)
    val courseContentsStatus = getContentStatusFromDB(primaryFields ++ Map(config.contentId -> leafNodes.asScala.toList))
    Map(courseId -> computeProgress(Map(config.activityType -> "course", config.contextId -> s"cb:${primaryFields.get(config.batchId)}", config.activityId -> s"${primaryFields.get(config.courseId)}"), leafNodes, courseContentsStatus, csFromEvent))
  }

  def readFromDB(columns: mutable.Map[String, AnyRef], keySpace: String, table: String): List[Row] = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(keySpace, table).
      where()
    columns.map(col => {
      if (col._2.isInstanceOf[List[Any]]) {
        selectWhere.and(QueryBuilder.in(col._1, col._2.asInstanceOf[List[_]].asJava))
      } else {
        selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    println("selectWhereselectWhere" + selectWhere.toString)
    cassandraUtil.find(selectWhere.toString).asScala.toList
  }

  def getQuery(progressColumns: Progress, keySpace: String, table: String): Update.Where = {
    QueryBuilder.update(keySpace, table)
      .`with`(QueryBuilder.set(config.agg, progressColumns.agg.asJava))
      .and(QueryBuilder.set(config.aggLastUpdated, progressColumns.agg_last_updated.asJava))
      .where(QueryBuilder.eq(config.activityId, progressColumns.activity_id))
      .and(QueryBuilder.eq(config.activityType, progressColumns.activity_type))
      .and(QueryBuilder.eq(config.contextId, progressColumns.context_id))
  }

  def getDataFromRedis(key: String, metrics: Metrics): util.List[String] = {
    metrics.incCounter(config.cacheHitCount)
    redisCache.lRangeWithRetry(key)
  }

  def computeProgress(cols: Map[String, AnyRef],
                      leafNodes: util.List[String],
                      csFromDB: Map[String, Int],
                      csFromEvent: mutable.Map[String, Int]): Progress = {
    val unionKeys = csFromEvent.keySet.union(csFromDB.keySet)
    val mergedContentStatus: Map[String, Int] = unionKeys.map { key =>
      (key -> (if (csFromEvent.get(key).getOrElse(0) >= csFromDB.get(key).getOrElse(0)) csFromEvent.get(key).getOrElse(0)
      else csFromDB.get(key).getOrElse(0)))
    }.toMap.filter(value => value._2 == config.completedStatusCode).filter(requiredNodes => leafNodes.contains(requiredNodes._1))
    val agg = Map(config.progress -> ((mergedContentStatus.size.toFloat / leafNodes.size().toFloat) * 100).toInt)
    val aggUpdatedOn = Map(config.progress -> new DateTime().getMillis)
    Progress(cols.get(config.activityType).getOrElse(null).asInstanceOf[String], cols.get(config.activityId).getOrElse(null).asInstanceOf[String], cols.get(config.contextId).getOrElse(null).asInstanceOf[String], agg, aggUpdatedOn)
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
      if (csFromEvent.get(content.get("contentId").asInstanceOf[String]).getOrElse(0) < content.get("status").asInstanceOf[Double].toInt) {
        csFromEvent.put(content.get("contentId").asInstanceOf[String], content.get("status").asInstanceOf[Double].toInt)
      }
    })
    csFromEvent
  }

  def getContentStatusFromDB(primaryFields: mutable.Map[String, AnyRef]): Map[String, Int] = {
    Option(readFromDB(primaryFields, config.dbKeyspace, "content_consumption"))
      .toList.flatMap(list => list.map(res => mutable.Map(res.getObject(config.contentId) -> res.getObject("status")).asInstanceOf[mutable.Map[String, Int]])).flatten.toMap
  }

  def writeToDb(query: String, metrics: Metrics): Unit = {
    cassandraUtil.upsert(query)
    metrics.incCounter(config.successEventCount)
    metrics.incCounter(config.dbUpdateCount)
  }
}
