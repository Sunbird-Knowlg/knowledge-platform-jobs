package org.sunbird.job.functions

import java.util
import java.util.UUID

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
    try {
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
          logger.info("Certificate generate event successfully sent to next topic")
          metrics.incCounter(config.successEventCount)
        })
      })
    } catch {
      case ex: Exception => {
        context.output(config.failedEventOutputTag, gson.toJson(edata))
        logger.info("Certificate generate event failed sent to next topic : " + ex.getMessage + " " + ex )
        metrics.incCounter(config.failedEventCount)
      }
    }
  }

  private def fetchCertTemplates(edata: util.Map[String, AnyRef])(implicit metrics: Metrics): util.Map[String, AnyRef] = {
    val certTemplates = CertificateDbService.readCertTemplates(edata)(metrics, cassandraUtil, config)
    println("cert fetch success : " + certTemplates.toString)
    EventValidator.validateTemplate(certTemplates, edata, config)(metrics)
    certTemplates
  }

  private def generateCertificateEvent(userId: String, template: CertTemplate, edata: util.Map[String, AnyRef], collectionCache: DataCache)
                                      (implicit metrics: Metrics): java.util.Map[String, AnyRef] = {
    println("generateCertificatesEvent called userId : " + userId)
    val generateRequest = IssueCertificateUtil.prepareGenerateRequest(edata, template, userId)(config)
    val edataRequest = gson.fromJson(gson.toJson(generateRequest), new util.HashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    // generate certificate event edata
    val eventEdata = new CertificateEventGenerator(config)(metrics, cassandraUtil).prepareGenerateEventEdata(edataRequest, collectionCache)
    println("generateCertificateEvent : eventEdata : " + eventEdata)
    generateCertificateFinalEvent(eventEdata)
  }

  private def generateCertificateFinalEvent(edata: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    new util.HashMap[String, AnyRef](){{
      put("eid","BE_JOB_REQUEST")
      put("ets",System.currentTimeMillis().asInstanceOf[AnyRef])
      put("mid",s"LMS.${UUID.randomUUID().toString}")
      put("edata",edata)
      put("object",new util.HashMap[String, AnyRef]{{put("id",edata.get(config.userId).asInstanceOf[String])}})
      put("type","GenerateCertificate")
      put("context",new util.HashMap[String, AnyRef]{{put("pdata", new util.HashMap[String, AnyRef](){{put("ver","1.0")
        put("id","org.sunbird.platform")}})}})
      put("actor",new util.HashMap[String, AnyRef]{{put("id","Certificate Generator")
        put("type","System")}})
    }}
  }
}