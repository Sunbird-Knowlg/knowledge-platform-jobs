package org.sunbird.job.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.common.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.collectioncert.domain.Event
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.task.CollectionCertPreProcessorConfig
import org.sunbird.job.util.{CassandraUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class CollectionCertPreProcessorFn(config: CollectionCertPreProcessorConfig)
                                  (implicit val stringTypeInfo: TypeInformation[String],
                                   @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) with IssueCertificateHelper {

    private[this] val logger = LoggerFactory.getLogger(classOf[CollectionCertPreProcessorFn])
    private var cache: DataCache = _

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
        val redisConnect = new RedisConnect(config)
        cache = new DataCache(config, redisConnect, config.collectionCacheStore, List())
        cache.init()
    }

    override def close(): Unit = {
        cassandraUtil.close()
        cache.close()
        super.close()
    }

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.dbReadCount, config.dbUpdateCount, config.failedEventCount, config.skippedEventCount,
            config.cacheHitCount)
    }

    override def processElement(event: Event,
                                context: ProcessFunction[Event, String]#Context,
                                metrics: Metrics): Unit = {
        try {
            if(isValidEvent(event)) {
                val certTemplates = fetchTemplates(event)(metrics)
                if(!certTemplates.isEmpty) {
                    certTemplates.map(template => {
                        val certEvent = issueCertificate(event, template._2)(cassandraUtil, cache, metrics, config)
                        context.output(config.generateCertificateOutputTag, certEvent)
                    })
                } else {
                    println("no templates")
                }
            } else {
                println("Invalid request")
            }
        } catch {
            case ex: Exception => {
                context.output(config.failedEventOutputTag, ScalaJsonUtil.serialize(event.eData))
                logger.error("Certificate generate event failed sent to next topic : ", ex)
                metrics.incCounter(config.failedEventCount)
            }
        }
        
        
    }

    def isValidEvent(event: Event): Boolean = {
        config.issueCertificate.equalsIgnoreCase(event.action) && !event.batchId.isBlank && !event.courseId.isBlank && 
          !event.userId.isBlank
    }

    def fetchTemplates(event: Event)(implicit metrics: Metrics): Map[String, Map[String, String]] = {
        val query = QueryBuilder.select(config.certTemplates).from(config.keyspace, config.courseTable)
          .where(QueryBuilder.eq(config.dbCourseId, event.courseId)).and(QueryBuilder.eq(config.dbBatchId, event.batchId))
        
        val row: Row = cassandraUtil.findOne(query.toString)
        if(null != row && !row.isNull(config.certTemplates)) {
            val templates = row.getMap(config.certTemplates, TypeToken.of(classOf[String]), TypeTokens.mapOf(classOf[String], classOf[String]))
            templates.asScala.map(template => (template._1 -> template._2.asScala.toMap)).toMap
        }else {
            Map[String, Map[String, String]]()
        }
    }

    
}
