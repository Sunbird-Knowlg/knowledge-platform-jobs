package org.sunbird.job.functions

import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.domain.{CertificateGenerateEvent, EventObject}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

class CertificatePreProcessor(config: CertificatePreProcessorConfig)
                             (implicit val stringTypeInfo: TypeInformation[String],
                              @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CertificatePreProcessor])
  lazy private val gson = new Gson()
  private var collectionCache: DataCache = _

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbReadCount, config.dbUpdateCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    val redisConnect = new RedisConnect(config)
    collectionCache = new DataCache(config, redisConnect, config.collectionCacheStore, List())
    collectionCache.init()
  }

  override def close(): Unit = {
    cassandraUtil.close()
    collectionCache.close()
    super.close()
  }

  override def processElement(event: util.Map[String, AnyRef], context: ProcessFunction[util.Map[String, AnyRef], String]#Context, metrics: Metrics) {
    val edata: util.Map[String, AnyRef] = event.get(config.eData).asInstanceOf[util.Map[String, AnyRef]]
    println("edata : " + edata)
    if (EventValidator.isValidEvent(edata, config)) {
      // prepare generate event request
      new IssueCertificateRequestGenerator(config)(metrics,cassandraUtil).prepareEventData(edata, collectionCache)
      // generate certificate event
      val certEvent: CertificateGenerateEvent = generateCertificateEvent(edata)
      // send event to next topic to generate certificate
//      context.output(config.generateCertificateOutputTag, gson.toJson(certEvent))
      metrics.incCounter(config.successEventCount)
    } else {
      logger.error("Validation failed for certificate event : batchId,courseId and/or userIds are empty")
      metrics.incCounter(config.skippedEventCount)
    }
    metrics.incCounter(config.totalEventsCount)
  }

  private def generateCertificateEvent(edata: util.Map[String, AnyRef]): CertificateGenerateEvent = {
    CertificateGenerateEvent(
      edata = edata,
      `object` = EventObject(id = edata.get(config.userId).asInstanceOf[String], `type` = "GenerateCertificate")
    )
  }

}