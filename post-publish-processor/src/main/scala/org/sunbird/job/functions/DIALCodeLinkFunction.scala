package org.sunbird.job.functions

import java.lang.reflect.Type
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._

class DIALCodeLinkFunction (config: PostPublishProcessorConfig, httpUtil: HttpUtil,
                            @transient var neo4JUtil: Neo4JUtil = null,
                            @transient var cassandraUtil: CassandraUtil = null)
                           (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DIALCodeLinkFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

  val graphId = "domain"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    if (neo4JUtil == null)
      neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(event: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    println("DIAL Code Link eData updated: " + event)
    val identifier = event.get("identifier").asInstanceOf[String]
    val reserved = event.getOrDefault("reservedDialcodes", reservedDialCodes(identifier))
      .asInstanceOf[java.util.Map[String, Int]]
    val dialCode = reserved.asScala.keys.head
    updateDIALToObject(identifier, dialCode)
  }

  override def metricsList(): List[String] = {
    List()
  }

  def reservedDialCodes(identifier: String): java.util.Map[String, Int] = {
    // TODO
    logger.info("Called reservedDialCodes for " + identifier)
    Map[String, Int]("Q7A5P7" -> 0).asJava
  }

  def updateDIALToObject(identifier: String, dialCode: String) = {
    val query = s"""MATCH (n:${graphId}{IL_UNIQUE_ID:"${identifier}"}) SET n.dialcodes=["$dialCode"];"""
    logger.info("Query: " + query)
    neo4JUtil.executeQuery(query)
  }
}
