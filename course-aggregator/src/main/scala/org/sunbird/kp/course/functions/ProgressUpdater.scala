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
        val primaryCols = eventData.asScala.map(v => (v._1.toLowerCase, v._2))
          .filter(x => config.primaryCols.contains(x._1))
        val leafNodes = getDataFromRedis(key = s"${primaryCols.get("courseid").getOrElse(null)}:leafnodes", metrics)
        if (null != leafNodes && !leafNodes.isEmpty) {
          val csFromEvent = getContentStatusFromEvent(eventData)
          //val courseId = s"${primaryCols.get("courseid").getOrElse(null)}"
          getUnitProgres(csFromEvent, primaryCols, metrics)
            .map(unit => batch.add(getQuery(unit._2, config.dbKeyspace, "activity_agg")))
          getCourseProgres(csFromEvent, primaryCols, metrics)
            .map(course => batch.add(getQuery(course._2, config.dbKeyspace, "activity_agg")))
          //          val unitProgressMap = mutable.Map[String, Progress]()
          //          csFromEvent.map(contentId => {
          //            val unitLevelAncestors = Option(getDataFromRedis(s"$courseId:${contentId._1}:ancestors", metrics)).map(x => x.asScala.filter(_ > courseId)).getOrElse(List())
          //            unitLevelAncestors.map(unitId => {
          //              val unitLeafNodes: util.List[String] = Option(getDataFromRedis(s"${unitId}:leafnodes", metrics)).getOrElse(new util.LinkedList())
          //              val cols = Map("activityType" -> "course-unit", "contextId" -> s"cb:${primaryCols.get("batchid")}", "activityId" -> s"${unitId}")
          //              val unitContentsStatusFromDB = getContentStatusFromDB(primaryCols ++ Map("contentid" -> unitLeafNodes.asScala.toList))
          //              unitProgressMap += (unitId -> computeProgress(cols, unitLeafNodes, unitContentsStatusFromDB, csFromEvent, context))
          //            })
          //          })
          //          val courseContentsStatus = getContentStatusFromDB(primaryCols ++ Map("contentid" -> leafNodes.asScala.toList))
          //          val courseProgress = computeProgress(Map("activityType" -> "course", "contextId" -> s"cb:${primaryCols.get("batchid")}", "activityId" -> s"${primaryCols.get("courseid")}"), leafNodes, courseContentsStatus, csFromEvent, context)
          //          batch.add(getQuery(courseProgress, config.dbKeyspace, "activity_agg"))
          println("BatchQueryIs" + batch.toString)
          writeToDb(batch.toString, metrics)




          //                  val  progress = unitLevelComputation(s"${primaryCols.get("courseid").getOrElse(null)}",
          //                      csFromDBs,
          //                      csFromEvent,
          //                      primaryCols, metrics, context, leafNodes.size())
          //          //println("progressprogress" + progress)

          //  batch.add(progress)
          // val progress = computeProgress(leafNodes.size(), csFromDB.asScala, csFromEvent, primaryCols, context)
          //batch.add(getQuery(progress, keySpace = config.dbKeyspace, table = config.dbTable))
          //          try {
          //            writeToDb(query = batch.toString, metrics)
          //          } catch {
          //            case ex: Exception =>
          //              ex.printStackTrace()
          //              metrics.incCounter(config.failedEventCount)
          //              logger.error(s"Error While writing Data into database for this Batch:${primaryCols.get("batchid")}, courseId:${primaryCols.get("courseid")}, userId:${primaryCols.get("userid")}")
          //              context.output(config.failedEventsOutputTag, gson.toJson(event))
          //          }
        } else {
          logger.debug(s"LeafNodes are not available in the redis for this Batch:${primaryCols.get("batchid")}, courseId:${primaryCols.get("courseid")}, userId:${primaryCols.get("userid")}")
          context.output(config.failedEventsOutputTag, gson.toJson(event))
          metrics.incCounter(config.failedEventCount)
        }
      }
    })
  }

  //    def unitLevelComputation(courseId: String,
  //                             csFromDB: Map[String, Int],
  //                             csFromEvent: mutable.Map[String, Int],
  //                             cols: mutable.Map[String, AnyRef],
  //                             metrics: Metrics,
  //                             context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context,
  //                             courseLeafNodesSize:Int) = {
  //     val progress = csFromEvent.map(contentId => {
  //        val unitLevelAncestors = Option(getDataFromRedis(s"$courseId:${contentId._1}:ancestors", metrics)).map(x => x.asScala.filter(_ > courseId)).getOrElse(List())
  //        unitLevelAncestors.map(unitId => {
  //          val unitLeafNodes: util.List[String] = Option(getDataFromRedis(s"${unitId}:leafnodes", metrics)).getOrElse(new util.LinkedList())
  //           computeProgress(Map("activityType" -> "course-unit", "contextId" -> s"cb:${cols.get("batchid")}", "activityId" -> s"${unitId}"), unitLeafNodes.size(), csFromDB, csFromEvent, context)
  //        })
  //      })
  //      println("progressprogress" + progress)
  //      progress
  //
  //      //computeProgress(Map("activityType" -> "course", "contextId" -> s"cb:${cols.get("batchid")}", "activityId" -> s"${cols.get("courseid")}"), courseLeafNodesSize, csFromDB, csFromEvent, context)
  //    }

  def getUnitProgres(csFromEvent: mutable.HashMap[String, Int], primaryCols: mutable.Map[String, AnyRef], metrics: Metrics): mutable.Map[String, Progress] = {
    val unitProgressMap = mutable.Map[String, Progress]()
    val courseId = s"${primaryCols.get("courseid").getOrElse(null)}"
    csFromEvent.map(contentId => {
      val unitLevelAncestors = Option(getDataFromRedis(s"$courseId:${contentId._1}:ancestors", metrics)).map(x => x.asScala.filter(_ > courseId)).getOrElse(List())
      unitLevelAncestors.map(unitId => {
        if (null != unitProgressMap.get(unitId).getOrElse(null)) {
          val unitLeafNodes: util.List[String] = Option(getDataFromRedis(s"${unitId}:leafnodes", metrics)).getOrElse(new util.LinkedList())
          val cols = Map("activityType" -> "course-unit", "contextId" -> s"cb:${primaryCols.get("batchid")}", "activityId" -> s"${unitId}")
          val unitContentsStatusFromDB = getContentStatusFromDB(primaryCols ++ Map("contentid" -> unitLeafNodes.asScala.toList))
          unitProgressMap += (unitId -> computeProgress(cols, unitLeafNodes, unitContentsStatusFromDB, csFromEvent))
        }
      })
    })
    unitProgressMap
  }

  def getCourseProgres(csFromEvent: mutable.HashMap[String, Int], primaryCols: mutable.Map[String, AnyRef], metrics: Metrics): Map[String, Progress] = {
    val courseId = s"${primaryCols.get("courseid").getOrElse(null)}"
    val leafNodes = getDataFromRedis(key = s"$courseId:leafnodes", metrics)
    val courseContentsStatus = getContentStatusFromDB(primaryCols ++ Map("contentid" -> leafNodes.asScala.toList))
    Map(courseId -> computeProgress(Map("activityType" -> "course", "contextId" -> s"cb:${primaryCols.get("batchid")}", "activityId" -> s"${primaryCols.get("courseid")}"), leafNodes, courseContentsStatus, csFromEvent))
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
      .`with`(QueryBuilder.set("agg", progressColumns.agg.asJava))
      .and(QueryBuilder.set("agg_last_updated", progressColumns.agg_last_updated.asJava))
      .where(QueryBuilder.eq("activity_id", progressColumns.activity_id))
      .and(QueryBuilder.eq("activity_type", progressColumns.activity_type))
      .and(QueryBuilder.eq("context_id", progressColumns.context_id))

  }

  def getDataFromRedis(key: String, metrics: Metrics): util.List[String] = {
    metrics.incCounter(config.cacheHitCount)
    redisCache.lRangeWithRetry(key)
  }

  def computeProgress(cols: Map[String, AnyRef],
                      leafNodes: util.List[String],
                      csFromDB: Map[String, Int],
                      csFromEvent: mutable.Map[String, Int]): Progress = {
    // Option(csFromDB).getOrElse(context.output(config.successEventOutputTag, gson.toJson(TelemetryEvent(eid = "START", mid = s"course-${primaryCols.get("batchid")}_${primaryCols.get("userid")}_start"))))
    val unionKeys = csFromEvent.keySet.union(csFromDB.keySet)
    val mergedContentStatus: Map[String, Int] = unionKeys.map { k =>
      (k -> (if (csFromEvent.get(k).getOrElse(0) >= csFromDB.get(k).getOrElse(0)) csFromEvent.get(k).getOrElse(0)
      else csFromDB.get(k).getOrElse(0)))
    }.toMap.filter(value => value._2 == config.completedStatusCode).filter(requiredNodes => leafNodes.contains(requiredNodes._1))
    val agg = Map("progress" -> ((mergedContentStatus.size.toFloat / leafNodes.size().toFloat) * 100).toInt)
    val aggUpdatedOn = Map("progress" -> new DateTime().getMillis)
    Progress(cols.get("activityType").getOrElse(null).asInstanceOf[String], cols.get("activityId").getOrElse(null).asInstanceOf[String], cols.get("contextId").getOrElse(null).asInstanceOf[String], agg, aggUpdatedOn)
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

  def getContentStatusFromDB(primaryCols: mutable.Map[String, AnyRef]): Map[String, Int] = {
    Option(readFromDB(primaryCols, config.dbKeyspace, "content_consumption"))
      .toList.flatMap(list => list.map(res => mutable.Map(res.getObject("contentid") -> res.getObject("status")).asInstanceOf[mutable.Map[String, Int]])).flatten.toMap
  }

  def writeToDb(query: String, metrics: Metrics): Unit = {
    cassandraUtil.upsert(query)
    metrics.incCounter(config.successEventCount)
    metrics.incCounter(config.dbUpdateCount)
  }
}
