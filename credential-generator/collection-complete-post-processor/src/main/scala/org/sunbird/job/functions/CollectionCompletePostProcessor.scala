package org.sunbird.job.functions

import java.util
import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.collectioncomplete.domain.Event
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.domain.CertTemplate
import org.sunbird.job.task.CollectionCompletePostProcessorConfig
import org.sunbird.job.util.{CassandraUtil, JSONUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters

class CollectionCompletePostProcessor(config: CollectionCompletePostProcessorConfig)
                                     (implicit val stringTypeInfo: TypeInformation[String],
                              @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CollectionCompletePostProcessor])
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

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics) = {
    if (StringUtils.equalsIgnoreCase(event.action, config.issueCertificate) &&
      StringUtils.isNotBlank(event.courseId) && StringUtils.isNotBlank(event.batchId) && !event.userIds.isEmpty) {
      // prepare generate event request and send to next topic
      prepareEventData(event, collectionCache, context)(metrics, config)
    } else {
      logger.error("Validation failed for certificate event : batchId,courseId and/or userIds are empty")
      metrics.incCounter(config.skippedEventCount)
    }
    metrics.incCounter(config.totalEventsCount)
  }

  private def prepareEventData(event: Event, collectionCache: DataCache,
                               context: ProcessFunction[Event, String]#Context)
                              (implicit metrics: Metrics, config: CollectionCompletePostProcessorConfig) {
    try {
      val certTemplates = fetchCertTemplates(event)(metrics)
      certTemplates.map(template => {
        //validate criteria
        val certTemplate = certTemplates.get(template._1).asInstanceOf[Map[String, AnyRef]]
        val usersToIssue = CertificateUserUtil.getUserIdsBasedOnCriteria(certTemplate, event)
        val templateUrl = certTemplate.getOrElse(config.url, "").asInstanceOf[String]
        if(StringUtils.isBlank(templateUrl) || !StringUtils.endsWith(templateUrl, ".svg")) {
          logger.info("Invalid template: Certificate generate event is skipped: " + event.eData)
          metrics.incCounter(config.skippedEventCount)
        } else {
          //iterate over users and send to generate event method
          val template = IssueCertificateUtil.prepareTemplate(certTemplate)(config)
          println("prepareTemplate output: " + template)
          usersToIssue.foreach(user => {
            val certEvent = generateCertificateEvent(user, template, event.eData, collectionCache)
            val eventStr = JSONUtil.serialize(certEvent)
            println("final event send to next topic : " + eventStr)
            context.output(config.generateCertificateOutputTag, eventStr)
            logger.info("Certificate generate event successfully sent to next topic")
            metrics.incCounter(config.successEventCount)
          })
        }
      })
    } catch {
      case ex: Exception => {
        context.output(config.failedEventOutputTag, JSONUtil.serialize(event.eData))
        logger.info("Certificate generate event failed sent to next topic : " + ex.getMessage + " " + ex )
        metrics.incCounter(config.failedEventCount)
      }
    }
  }

  private def fetchCertTemplates(event: Event)(implicit metrics: Metrics): Map[String, Map[String, String]] = {
    val certTemplates = CertificateDbService.readCertTemplates(event.batchId, event.courseId)(metrics, cassandraUtil, config)
    if (certTemplates.isEmpty) {
      metrics.incCounter(config.skippedEventCount)
      logger.error(
        "Certificate template is not available for batchId : " + event.batchId + " and courseId : " + event.courseId)
    }
      certTemplates
  }

  private def generateCertificateEvent(userId: String, template: CertTemplate, edata: util.Map[String, AnyRef], collectionCache: DataCache)
                                      (implicit metrics: Metrics): java.util.Map[String, AnyRef] = {
    println("generateCertificatesEvent called userId : " + userId +":: "+ template)
    val generateRequest = IssueCertificateUtil.prepareGenerateRequest(edata, template, userId)(config)
    println("prepareGenerateRequest output: " + convertToMap(generateRequest))
    val edataRequest = convertToMap(generateRequest)
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
      put("object",new util.HashMap[String, AnyRef]{{put("id",edata.get(config.userId).asInstanceOf[String])
        put("type","GenerateCertificate")
      }})
      put("context",new util.HashMap[String, AnyRef]{{put("pdata", new util.HashMap[String, AnyRef](){{put("ver","1.0")
        put("id","org.sunbird.platform")}})}})
      put("actor",new util.HashMap[String, AnyRef]{{put("id","Certificate Generator")
        put("type","System")}})
    }}
  }

  def convertToMap(cc: AnyRef) = {
    JavaConverters.mapAsJavaMap(cc.getClass.getDeclaredFields.foldLeft (Map.empty[String, AnyRef]) { (a, f) => f.setAccessible(true)
      a + (f.getName -> f.get(cc)) })
  }

}