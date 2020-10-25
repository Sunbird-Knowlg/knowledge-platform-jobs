package org.sunbird.job.functions

import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.domain.{CertTemplate, CertificateGenerateEvent, EventObject}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class CertificatePreProcessor(config: CertificatePreProcessorConfig)
                             (implicit val stringTypeInfo: TypeInformation[String],
                              @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  lazy private val gson = new Gson()
  private[this] val logger = LoggerFactory.getLogger(classOf[CertificatePreProcessor])
  private var collectionCache: DataCache = _

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbReadCount, config.cacheReadCount)
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
      // prepare generate event request and send to next topic
      prepareEventData(edata, collectionCache, context)(metrics, config)
    } else {
      logger.error("Validation failed for certificate event : batchId,courseId and/or userIds are empty")
      metrics.incCounter(config.skippedEventCount)
    }
    metrics.incCounter(config.totalEventsCount)
  }

  private def prepareEventData(edata: util.Map[String, AnyRef], collectionCache: DataCache,
                               context: ProcessFunction[util.Map[String, AnyRef], String]#Context)
                              (implicit metrics: Metrics, config: CertificatePreProcessorConfig) {
    val certTemplates = fetchCertTemplates(edata)(metrics)
    certTemplates.keySet().forEach(templateId => {
      //validate criteria
      val certTemplate = certTemplates.get(templateId).asInstanceOf[util.Map[String, AnyRef]]
      val usersToIssue = CertificateUserUtil.getUserIdsBasedOnCriteria(certTemplate, edata)
      //iterate over users and send to generate event method
      val template = IssueCertificateUtil.prepareTemplate(certTemplate)(config)
      usersToIssue.foreach(user => {
        val certEvent = generateCertificateEvent(user, template, edata, collectionCache)
        println("final event send to next topic : " + gson.toJson(certEvent))
        context.output(config.generateCertificateOutputTag, gson.toJson(certEvent))
        logger.info("Certificate generate event successfully send to next topic")
        metrics.incCounter(config.successEventCount)
      })
    })
  }

  private def fetchCertTemplates(edata: util.Map[String, AnyRef])(implicit metrics: Metrics): util.Map[String, AnyRef] = {
    val certTemplates = CertificateDbService.readCertTemplates(edata)(metrics, cassandraUtil, config)
    println("cert fetch success : " + certTemplates.toString)
    EventValidator.validateTemplate(certTemplates, edata, config)(metrics)
    certTemplates
  }

  private def generateCertificateEvent(userId: String, template: CertTemplate, edata: util.Map[String, AnyRef], collectionCache: DataCache)
                                      (implicit metrics: Metrics): CertificateGenerateEvent = {
    println("generateCertificatesEvent called userId : " + userId)
    val generateRequest = IssueCertificateUtil.prepareGenerateRequest(edata, template, userId)(config)
    val edataRequest = generateRequest.getClass.getDeclaredFields.map(_.getName).zip(generateRequest.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava
    // generate certificate event edata
    val eventEdata = new CertificateEventGenerator(config)(metrics, cassandraUtil).prepareGenerateEventEdata(edataRequest, collectionCache)
    println("generateCertificateEvent : eventEdata : " + eventEdata)
    generateCertificateEvent(edata)
  }

  private def generateCertificateEvent(edata: util.Map[String, AnyRef]): CertificateGenerateEvent = {
    CertificateGenerateEvent(
      edata = edata,
      `object` = EventObject(id = edata.get(config.userId).asInstanceOf[String], `type` = "GenerateCertificate")
    )
  }
}