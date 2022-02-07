package org.sunbird.job.content.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.content.task.InteractiveContentPublishConfig
import org.sunbird.job.publish.core.PublishCoreMetadata
import org.sunbird.job.publish.helpers.EventGenerator
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type

class InteractiveContentFunction(config: InteractiveContentPublishConfig)
                                (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config)  {

  private[this] val logger = LoggerFactory.getLogger(classOf[InteractiveContentFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  //private val readerConfig = ExtDataConfig(config.contentKeyspaceName, config.contentTableName)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
   // ec = ExecutionContexts.global
  }

  override def close(): Unit = {
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.publishChainEventCount, config.publishChainSuccessEventCount, config.publishChainFailedEventCount)
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    try {
      implicit val oec = new OntologyEngineContext()
      metrics.incCounter(config.publishChainEventCount)
      for( publishChainEvent : Map[String, AnyRef] <- event.publishChain){
        logger.info("PublishEventRouter :: Sending Publish Chain For Publish Having Identifier: " + publishChainEvent.getOrElse("identifier", ""))
        if(publishChainEvent.getOrElse("state","").equals("Processing")) {
          val publishCoreMetaData : PublishCoreMetadata = new PublishCoreMetadata(publishChainEvent.getOrElse("identifier", "").toString, publishChainEvent.getOrElse("pkgVersion","0").toString,event.publishType, event.eData, event.context,event.obj,event.publishChain)
          publishChainEvent.getOrElse("objectType","") match {
            case "Content" | "ContentImage" => {
              EventGenerator.pushPublishEvent(publishChainEvent,publishCoreMetaData,config.contentPublishTopic,"content-publish")
              return
            }
            case "QuestionSet" | "QuestionSetImage" => {
              EventGenerator.pushPublishEvent(publishChainEvent,publishCoreMetaData,config.questionSetTopic,"questionset-publish")
              return
            }
            case _ => {
              logger.info("Invalid object type in publish chain events for identifier : " + publishChainEvent.getOrElse("identifier", ""))
            }
          }
        }
      }
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.publishChainFailedEventCount)
        logger.info("Publish Chain Event publishing failed for : " + event.obj.getOrElse("id",""))
        throw ex
    }
  }

}
