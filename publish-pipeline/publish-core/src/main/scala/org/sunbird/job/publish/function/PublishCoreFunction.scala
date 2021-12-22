package org.sunbird.job.publish.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.core.{DefinitionConfig, PublishMetadata}
import org.sunbird.job.publish.helpers.{EcarPackageType, EventGenerator}
import org.sunbird.job.publish.task.PublishCoreConfig
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil, OntologyEngineContext}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import scala.concurrent.ExecutionContext

class PublishCoreFunction(config: PublishCoreConfig, httpUtil: HttpUtil,
                          @transient var neo4JUtil: Neo4JUtil = null,
                          @transient var cassandraUtil: CassandraUtil = null,
                          @transient var cloudStorageUtil: CloudStorageUtil = null,
                          @transient var definitionCache: DefinitionCache = null,
                          @transient var definitionConfig: DefinitionConfig = null)
                         (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config)   {

  private[this] val logger = LoggerFactory.getLogger(classOf[PublishCoreFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

  @transient var ec: ExecutionContext = _
  private val pkgTypes = List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE.toString)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def metricsList(): List[String] = {
    List(config.publishChainEventCount, config.publishChainSuccessEventCount, config.publishChainFailedEventCount)
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
  }



  override def processElement(publishMetadata: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
    implicit val oec = new OntologyEngineContext()
    for( publishChainEvent : Map[String, AnyRef] <- publishMetadata.publishChain){
      logger.info("PublishEventRouter :: Sending Publish Chain For Publish Having Identifier: " + publishChainEvent.getOrElse("identifier", ""))

      if(publishChainEvent.getOrElse("state","").equals("Processing")) {
        publishChainEvent.getOrElse("objectType","") match {
          case "Content" | "ContentImage" => {
            EventGenerator.pushPublishEvent(publishChainEvent,publishMetadata,config.contentTopic,"content-publish")
            return
          }
          case "QuestionSet" | "QuestionSetImage" => {
            EventGenerator.pushPublishEvent(publishChainEvent,publishMetadata,config.questionSetTopic,"questionset-publish")
            return
          }
          case _ => {
            logger.info("Invalid object type in publish chain events for identifier : " + publishChainEvent.getOrElse("identifier", ""))
          }
        }
      }
    }
  }
}
