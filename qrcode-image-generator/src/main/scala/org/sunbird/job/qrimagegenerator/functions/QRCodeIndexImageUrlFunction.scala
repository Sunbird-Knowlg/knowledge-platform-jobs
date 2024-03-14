package org.sunbird.job.qrimagegenerator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.qrimagegenerator.domain.Event
import org.sunbird.job.qrimagegenerator.task.QRCodeImageGeneratorConfig
import org.sunbird.job.qrimagegenerator.util.QRCodeImageGeneratorUtil
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, ElasticSearchUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.mutable

class QRCodeIndexImageUrlFunction(config: QRCodeImageGeneratorConfig,
                                  @transient var cassandraUtil: CassandraUtil = null,
                                  @transient var cloudStorageUtil: CloudStorageUtil = null,
                                  @transient var esUtil: ElasticSearchUtil = null,
                                  @transient var qRCodeImageGeneratorUtil: QRCodeImageGeneratorUtil = null)
                                 (implicit val stringTypeInfo: TypeInformation[String])  extends BaseProcessFunction[Event, String](config) {

  private val logger = LoggerFactory.getLogger(classOf[QRCodeIndexImageUrlFunction])

  override def open(parameters: Configuration): Unit = {
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    esUtil = new ElasticSearchUtil(config.esConnectionInfo, config.dialcodeExternalIndex, config.dialcodeExternalIndexType)
    qRCodeImageGeneratorUtil = new QRCodeImageGeneratorUtil(config, cassandraUtil, cloudStorageUtil, esUtil)
    super.open(parameters)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def metricsList(): List[String] = {
    List()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
        event.dialCodes.foreach { dialcode =>
            try {
              val text = dialcode("text").asInstanceOf[String]
              qRCodeImageGeneratorUtil.indexImageInDocument(text)(esUtil, cassandraUtil)
            } catch {
              case e: Exception => e.printStackTrace()
                throw new InvalidEventException(e.getMessage)
            }
          }

  }

}
