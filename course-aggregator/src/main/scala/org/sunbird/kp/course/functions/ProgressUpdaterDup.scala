//package org.sunbird.kp.course.functions
//
//import java.lang.reflect.Type
//import java.util
//
//import com.datastax.driver.core.Row
//import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
//import com.google.gson.Gson
//import com.google.gson.reflect.TypeToken
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.joda.time.DateTime
//import org.slf4j.LoggerFactory
//import org.sunbird.async.core.cache.{DataCache, RedisConnect}
//import org.sunbird.async.core.job.{BaseProcessFunction, Metrics}
//import org.sunbird.async.core.util.CassandraUtil
//import org.sunbird.kp.course.domain.{Progress, TelemetryEvent}
//import org.sunbird.kp.course.task.CourseAggregatorConfig
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//
//
//class ProgressUpdater(config: CourseAggregatorConfig)(implicit val stringTypeInfo: TypeInformation[String],
//                                                 @transient var cassandraUtil: CassandraUtil = null
//)
//  extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {
//
//  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
//  private[this] val logger = LoggerFactory.getLogger(classOf[ProgressUpdater])
//  private var dataCache: DataCache = _
//  lazy private val gson = new Gson()
//
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
//    dataCache = new DataCache(config, new RedisConnect(config), config.leafNodesStore, List())
//    dataCache.init()
//  }
//
//  override def close(): Unit = {
//    super.close()
//  }
//
//  /**
//   * List of metrics capturing
//   *
//   * @return
//   */
//  override def metricsList(): List[String] = {
//    List(config.successEventCount, config.failedEventCount, config.totalEventsCount, config.dbUpdateCount)
//  }
//
//  /**
//   * Method to process course progress events
//   *
//   * @param event -
//   * @param context
//   */
//  override def processElement(event: util.Map[String, AnyRef],
//                              context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
//                              metrics: Metrics): Unit = {
//    metrics.incCounter(config.totalEventsCount)
//    val eventData = event.get("edata").asInstanceOf[util.Map[String, AnyRef]]
//    val csFromEvent = new mutable.HashMap[String, Int]()
//    val primaryColss = eventData.asScala.map(v => (v._1.toLowerCase, v._2)).filter(x=> x._1.contains(config.primaryCols))
//    println("primaryColss" + primaryColss)
//     val primaryCols = Map(
//        "courseid" -> eventData.get("courseId").asInstanceOf[String],
//        "batchid" -> eventData.get("batchId").asInstanceOf[String],
//        "userid" -> eventData.get("userId").asInstanceOf[String]
//      )
//
//    val contentsList = eventData.get("contents").asInstanceOf[util.List[util.Map[String, AnyRef]]]
//    contentsList.forEach(content => {
//      csFromEvent.put(content.get("contentId").asInstanceOf[String], content.get("status").asInstanceOf[Double].toInt)
//    })
//    val dbResponse = readFromDB(primaryCols, config.dbKeyspace, config.dbTable)
//    val csFromDB = Option(dbResponse).map(res => {
//      res.getObject("contentstatus").asInstanceOf[util.Map[String, Int]]
//    }).getOrElse(new util.HashMap[String, Int]())
//    // Get the LeafNodes from Redis
//    val leafNodes = getLeafNodes(key = s"${primaryCols.get("courseid")}:leafnodes")
//    if (null != leafNodes && !leafNodes.isEmpty) {
//      val progress = computeProgress(leafNodes.size(), csFromDB.asScala, csFromEvent, primaryCols, context)
//      try {
//        writeToDB(progress, config.dbKeyspace, config.dbTable)
//        metrics.incCounter(config.successEventCount)
//      } catch {
//        case ex: Exception =>
//          ex.printStackTrace()
//          metrics.incCounter(config.failedEventCount)
//          logger.error(s"Error While writing Data into database for this Batch:${primaryCols.get("batchid")}, courseId:${primaryCols.get("courseid")}, userId:${primaryCols.get("userid")}")
//          context.output(config.failedEventsOutputTag, gson.toJson(event))
//      }
//      // Update the progress to DB
//    } else {
//      logger.debug(s"LeafNodes are not available in the redis for this Batch:${primaryCols.get("batchid")}, courseId:${primaryCols.get("courseid")}, userId:${primaryCols.get("userid")}")
//      context.output(config.failedEventsOutputTag, gson.toJson(event))
//      metrics.incCounter(config.failedEventCount)
//    }
//
//  }
//
//  def readFromDB(columns: Map[String, AnyRef], keySpace: String, table: String): Row = {
//    val selectWhere: Select.Where = QueryBuilder.select().all()
//      .from(keySpace, table).
//      where()
//    columns.map(col => {
//      if (col._2.isInstanceOf[List[Any]]) {
//        selectWhere.and(QueryBuilder.in(col._1, col._2.asInstanceOf[List[_]]))
//      } else {
//        selectWhere.and(QueryBuilder.eq(col._1, col._2))
//      }
//    })
//    cassandraUtil.findOne(selectWhere.toString)
//  }
//
//  def writeToDB(progressColumns: Progress, keySpace: String, table: String): Unit = {
//    val query = QueryBuilder.update(keySpace, table)
//      .`with`(QueryBuilder.set("progress", progressColumns.progress))
//      .and(QueryBuilder.set("contentstatus", progressColumns.contentStatus.asJava)).and(QueryBuilder.set("completionpercentage", progressColumns.completionPercentage))
//      .and(QueryBuilder.set("completedon", progressColumns.completedOn.getOrElse(null))).and(QueryBuilder.set("status", progressColumns.status))
//      .where(QueryBuilder.eq("batchid", progressColumns.batchId))
//      .and(QueryBuilder.eq("userid", progressColumns.userId)).and(QueryBuilder.eq("courseid", progressColumns.courseId))
//    cassandraUtil.upsert(query.toString)
//  }
//
//  def getLeafNodes(key: String): util.List[String] = {
//    dataCache.lRangeWithRetry(key)
//  }
//
//  def computeProgress(leafNodesSize: Int,
//                      csFromDB: mutable.Map[String, Int],
//                      csFromEvent: mutable.Map[String, Int],
//                      primaryCols: Map[String, String],
//                      context: ProcessFunction[util.Map[String, AnyRef], String]#Context): Progress = {
//    // Generating TELEMETRY START Event When ContentStatus IN DB is Empty or Null
//    Option(csFromDB).getOrElse(context.output(config.successEventOutputTag, gson.toJson(TelemetryEvent(eid = "START", mid = s"course-${primaryCols.get("batchid")}_${primaryCols.get("userid")}_start"))))
//    val unionKeys = csFromEvent.keySet.union(csFromDB.keySet)
//    val mergedContentStatus: Map[String, Int] = unionKeys.map { k =>
//      (k -> (if (csFromEvent.get(k).getOrElse(0) >= csFromDB.get(k).getOrElse(0)) csFromEvent.get(k).getOrElse(0)
//      else csFromDB.get(k).getOrElse(0)))
//    }.toMap.filter(value => value._2 == config.completedStatusCode)
//    val completionPercentage = ((mergedContentStatus.size.toFloat / leafNodesSize.toFloat) * 100).toInt
//    if (completionPercentage == config.completionPercentage) {
//      // Generating TELEMETRY END Event When completionPercentage is config.completionPercentage or 100%
//      context.output(config.successEventOutputTag, gson.toJson(TelemetryEvent(eid = "END", mid = s"course-${primaryCols.get("batchid")}_${primaryCols.get("userid")}_compelete")))
//      Progress(primaryCols.get("batchid").get, primaryCols.get("courseid").get, primaryCols.get("userid").get, status = config.completedStatusCode, completedOn = Some(new DateTime().getMillis), contentStatus = mergedContentStatus, progress = mergedContentStatus.size, completionPercentage = completionPercentage)
//    } else {
//      Progress(primaryCols.get("batchid").get, primaryCols.get("courseid").get, primaryCols.get("userid").get, status = config.inCompleteStatusCode, completedOn = None, contentStatus = mergedContentStatus, progress = mergedContentStatus.size, completionPercentage = completionPercentage)
//    }
//  }
//
//}
