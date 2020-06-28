package org.sunbird.kp.course.functions

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
import org.sunbird.kp.course.domain.{Progress, TelemetryEvent}
import org.sunbird.kp.course.task.CourseAggregatorConfig

import scala.collection.JavaConverters._
import scala.collection.mutable

class ProgressUpdater(config: CourseAggregatorConfig)(implicit val stringTypeInfo: TypeInformation[String],
                                                      @transient var cassandraUtil: CassandraUtil = null
) extends WindowBaseProcessFunction[util.Map[String, AnyRef], String, String](config) {
  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ProgressUpdater])
  private var dataCache: DataCache = _
  lazy private val gson = new Gson()
  val actionType = "batch-enrolment-update"

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.totalEventsCount, config.dbUpdateCount, config.cacheHitCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    dataCache = new DataCache(config, new RedisConnect(config), config.leafNodesStore, List())
    dataCache.init()
  }

  override def close(): Unit = {
    super.close()
  }

  override def process(key: String,
                       context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context,
                       events: lang.Iterable[util.Map[String, AnyRef]], metrics: Metrics): Unit = {
    val batch = QueryBuilder.batch()
    events.forEach(event => {
      val eventData = event.get("edata").asInstanceOf[util.Map[String, AnyRef]]
      if (eventData.get("action") == actionType) {
        metrics.incCounter(config.totalEventsCount) // To Measure the number of batch-enrollment-updater events came.
        val csFromEvent = getContentStatsFromEvent(eventData)
        val primaryCols = eventData.asScala.map(v => (v._1.toLowerCase, v._2)).filter(x => config.primaryCols.contains(x._1)).asInstanceOf[mutable.Map[String, String]]
        val csFromDB = Option(readFromDB(primaryCols, config.dbKeyspace, config.dbTable)).map(res => {
          res.getObject("contentstatus").asInstanceOf[util.Map[String, Int]]
        }).getOrElse(new util.HashMap[String, Int]())

        // Get the LeafNodes from Redis
        val leafNodes = getLeafNodes(key = s"${primaryCols.get("courseid").getOrElse(null)}:leafnodes", metrics)
        println("leafNodesleafNodes" + leafNodes)
        if (null != leafNodes && !leafNodes.isEmpty) {
          val progress = computeProgress(leafNodes.size(), csFromDB.asScala, csFromEvent, primaryCols, context)
          batch.add(getQuery(progress, keySpace = config.dbKeyspace, table = config.dbTable))
          try {
            writeToDb(query = batch.toString, metrics)
          } catch {
            case ex: Exception =>
              println("ERRORIS" + ex.getMessage)
              ex.printStackTrace()
              metrics.incCounter(config.failedEventCount)
              logger.error(s"Error While writing Data into database for this Batch:${primaryCols.get("batchid")}, courseId:${primaryCols.get("courseid")}, userId:${primaryCols.get("userid")}")
              context.output(config.failedEventsOutputTag, gson.toJson(event))
          }
        } else {
          logger.debug(s"LeafNodes are not available in the redis for this Batch:${primaryCols.get("batchid")}, courseId:${primaryCols.get("courseid")}, userId:${primaryCols.get("userid")}")
          context.output(config.failedEventsOutputTag, gson.toJson(event))
          metrics.incCounter(config.failedEventCount)
        }
      }
    })
  }

  def readFromDB(columns: mutable.Map[String, String], keySpace: String, table: String): Row = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(keySpace, table).
      where()
    columns.map(col => {
      if (col._2.isInstanceOf[List[Any]]) {
        selectWhere.and(QueryBuilder.in(col._1, col._2.asInstanceOf[List[_]]))
      } else {
        selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    cassandraUtil.findOne(selectWhere.toString)
  }

  def getQuery(progressColumns: Progress, keySpace: String, table: String): Update.Where = {
    QueryBuilder.update(keySpace, table)
      .`with`(QueryBuilder.set("progress", progressColumns.progress))
      .and(QueryBuilder.set("contentstatus", progressColumns.contentStatus.asJava)).and(QueryBuilder.set("completionpercentage", progressColumns.completionPercentage))
      .and(QueryBuilder.set("completedon", progressColumns.completedOn.getOrElse(null))).and(QueryBuilder.set("status", progressColumns.status))
      .where(QueryBuilder.eq("batchid", progressColumns.batchId))
      .and(QueryBuilder.eq("userid", progressColumns.userId)).and(QueryBuilder.eq("courseid", progressColumns.courseId))
  }

  def getLeafNodes(key: String, metrics: Metrics): util.List[String] = {
    metrics.incCounter(config.cacheHitCount)
    val data = dataCache.lRangeWithRetry(key)
    println("DataIs" + gson.toJson(data))
    data
  }

  def computeProgress(leafNodesSize: Int,
                      csFromDB: mutable.Map[String, Int],
                      csFromEvent: mutable.Map[String, Int],
                      primaryCols: mutable.Map[String, String],
                      context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context): Progress = {
    // Generating TELEMETRY START Event When ContentStatus IN DB is Empty or Null
    Option(csFromDB).getOrElse(context.output(config.successEventOutputTag, gson.toJson(TelemetryEvent(eid = "START", mid = s"course-${primaryCols.get("batchid")}_${primaryCols.get("userid")}_start"))))
    println("csFromEvent")
    val unionKeys = csFromEvent.keySet.union(csFromDB.keySet)
    println("unionKeys" + unionKeys)
    val mergedContentStatus: Map[String, Int] = unionKeys.map { k =>
      (k -> (if (csFromEvent.get(k).getOrElse(0) >= csFromDB.get(k).getOrElse(0)) csFromEvent.get(k).getOrElse(0)
      else csFromDB.get(k).getOrElse(0)))
    }.toMap.filter(value => value._2 == config.completedStatusCode)
    println("mergedContentStatus" + mergedContentStatus)
    println("leafNodesSize" + leafNodesSize)
    val completionPercentage = ((mergedContentStatus.size.toFloat / leafNodesSize.toFloat) * 100).toInt
    if (completionPercentage == config.completionPercentage) {
      // Generating TELEMETRY END Event When completionPercentage is config.completionPercentage or 100%
      context.output(config.successEventOutputTag, gson.toJson(TelemetryEvent(eid = "END", mid = s"course-${primaryCols.get("batchid")}_${primaryCols.get("userid")}_compelete")))
      Progress(primaryCols.get("batchid").get, primaryCols.get("userid").get, primaryCols.get("courseid").get, status = config.completedStatusCode, completedOn = Some(new DateTime().getMillis), contentStatus = mergedContentStatus, progress = mergedContentStatus.size, completionPercentage = completionPercentage)
    } else {
      Progress(primaryCols.get("batchid").get, primaryCols.get("userid").get, primaryCols.get("courseid").get, status = config.inCompleteStatusCode, completedOn = None, contentStatus = mergedContentStatus, progress = mergedContentStatus.size, completionPercentage = completionPercentage)
    }
  }


  /**
   * Method to get the content status object in map format ex: (do_5874308329084 -> 2, do_59485345435 -> 3)
   * It always takes the highest precedence progress values for the contents ex: (do_5874308329084 -> 2, do_5874308329084 -> 1, do_59485345435 -> 3) => (do_5874308329084 -> 2, do_59485345435 -> 3)
   *
   * @param eventData
   * @return
   */
  def getContentStatsFromEvent(eventData: util.Map[String, AnyRef]): mutable.HashMap[String, Int] = {
    val csFromEvent = new mutable.HashMap[String, Int]()
    val contentsList = eventData.get("contents").asInstanceOf[util.List[util.Map[String, AnyRef]]]
    contentsList.forEach(content => {
      if (csFromEvent.get(content.get("contentId").asInstanceOf[String]).getOrElse(0) < content.get("status").asInstanceOf[Double].toInt) {
        csFromEvent.put(content.get("contentId").asInstanceOf[String], content.get("status").asInstanceOf[Double].toInt)
      }
    })
    csFromEvent
  }

  def writeToDb(query: String, metrics: Metrics): Unit = {
    println("Cassandra Query Is" + query)
    cassandraUtil.upsert(query)
    metrics.incCounter(config.successEventCount)
    metrics.incCounter(config.dbUpdateCount)
  }
}
