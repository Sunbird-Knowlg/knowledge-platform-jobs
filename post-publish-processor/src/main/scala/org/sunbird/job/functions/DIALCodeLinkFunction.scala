package org.sunbird.job.functions

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.models.ExtDataConfig
import org.sunbird.job.postpublish.helpers.{DialHelper}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._

class DIALCodeLinkFunction(config: PostPublishProcessorConfig, httpUtil: HttpUtil,
                           @transient var neo4JUtil: Neo4JUtil = null,
                           @transient var cassandraUtil: CassandraUtil = null)
                          (implicit val stringTypeInfo: TypeInformation[String])
    extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config)
        with DialHelper{

    private[this] val logger = LoggerFactory.getLogger(classOf[DIALCodeLinkFunction])

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
        neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
    }

    override def close(): Unit = {
        cassandraUtil.close()
        super.close()
    }

    override def processElement(edata: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
        logger.info(s"Link DIAL Code operation triggered with object : ${edata}")
        metrics.incCounter(config.dialLinkingCount)
        val identifier = edata.get("identifier").asInstanceOf[String]
        val dialcodes = fetchExistingDialcodes(edata)
        logger.info(s"Dialcodes fetched: ${dialcodes}") // temp
        if (dialcodes.isEmpty) {
            logger.info(s"No Dial Code found. Checking for Reserved Dialcodes.")
            var reservedDialCode: util.Map[String, Integer] = fetchExistingReservedDialcodes(edata)
            if (reservedDialCode.isEmpty) {
                logger.info(s"No Reserved Dial Code found. Sending request for Reserving Dialcode.")
                reservedDialCode = reserveDialCodes(edata, config)(httpUtil)
            }
            reservedDialCode.asScala.keys.headOption match {
                case Some(dialcode: String) => {
                    updateDIALToObject(identifier, dialcode)(neo4JUtil)
                    createQRGeneratorEvent(edata, dialcode, context, config)(metrics, ExtDataConfig(config.dialcodeKeyspaceName, config.dialcodeTableName), cassandraUtil)
                }
                case _ => logger.info(s"Couldn't reserve any dialcodes for object with identifier:${identifier}")
            }
        } else {
            if (validateQR(dialcodes.get(0))(ExtDataConfig(config.dialcodeKeyspaceName, config.dialcodeTableName), cassandraUtil)) {
                logger.info(s"Event Skipped. Target Object [ $identifier ] already has DIAL Code and its QR Image.| DIAL Codes : $dialcodes")
                metrics.incCounter(config.skippedEventCount)
            } else {
                createQRGeneratorEvent(edata, dialcodes.get(0), context, config)(metrics, ExtDataConfig(config.dialcodeKeyspaceName, config.dialcodeTableName), cassandraUtil)
            }
        }
    }

    override def metricsList(): List[String] = {
        List(config.dialLinkingCount, config.qrImageGeneratorEventCount, config.skippedEventCount)
    }

}
