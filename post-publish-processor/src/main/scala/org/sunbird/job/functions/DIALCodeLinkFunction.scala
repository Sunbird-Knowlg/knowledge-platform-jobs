package org.sunbird.job.functions

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.models.ExtDataConfig
import org.sunbird.job.postpublish.helpers.{DialUtility, QRUtility}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._

class DIALCodeLinkFunction(config: PostPublishProcessorConfig, httpUtil: HttpUtil,
                           @transient var neo4JUtil: Neo4JUtil = null,
                           @transient var cloudStorageUtil: CloudStorageUtil = null,
                           @transient var cassandraUtil: CassandraUtil = null)
                          (implicit val stringTypeInfo: TypeInformation[String])
    extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config)
        with DialUtility with QRUtility {

    private[this] val logger = LoggerFactory.getLogger(classOf[DIALCodeLinkFunction])

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
        neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
        cloudStorageUtil = new CloudStorageUtil(config)
    }

    override def close(): Unit = {
        cassandraUtil.close()
        super.close()
    }

    override def processElement(edata: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
        println("DIAL Code Link eData updated: " + edata)
        // TODO: Check if event contentType is valid
        val identifier = edata.get("identifier").asInstanceOf[String]
        val dialcodes = fetchExistingDialcodes(edata)(neo4JUtil)
        if (dialcodes.isEmpty) {
            val reserved = edata.getOrDefault("reservedDialcodes", fetchExistingReservedDialcodes(edata)(neo4JUtil)).asInstanceOf[java.util.Map[String, Int]]
            reserved.asScala.keys.headOption match {
                case Some(dialcode: String) => {
                    updateDIALToObject(identifier, dialcode)(neo4JUtil)
                    generateQRImage(edata.getOrDefault("channel", "").asInstanceOf[String], dialcode)(ExtDataConfig(config.dialcodeKeyspaceName, config.dialcodeTableName), cassandraUtil, cloudStorageUtil)
                }
                case _ => {
                    logger.info(s"Couldn't fetch any dialcodes for object with identifier:$identifier")
                    val reservedDialcodes = reserveDialCodes(edata)(httpUtil)
                    reservedDialcodes.asScala.keys.headOption match {
                        case Some(dialcode: String) => {
                            updateDIALToObject(identifier, dialcode)(neo4JUtil)
                            generateQRImage(edata.getOrDefault("channel", "").asInstanceOf[String], dialcode)(ExtDataConfig(config.dialcodeKeyspaceName, config.dialcodeTableName), cassandraUtil, cloudStorageUtil)
                        }
                        case _ => logger.info(s"Couldn't reserve any dialcodes for object with identifier:$identifier")
                    }
                }
            }
        } else {
            validateAndGenerateQR(dialcodes.get(0), identifier, edata.getOrDefault("channel", "").asInstanceOf[String])(ExtDataConfig(config.dialcodeKeyspaceName, config.dialcodeTableName), cassandraUtil, cloudStorageUtil)
        }

    }

    override def metricsList(): List[String] = {
        List(config.dialLinkingCount)
    }


}
