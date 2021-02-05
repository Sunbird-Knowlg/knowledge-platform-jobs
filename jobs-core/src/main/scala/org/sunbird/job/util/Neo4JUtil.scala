package org.sunbird.job.util

import org.neo4j.driver.v1.{Config, GraphDatabase, StatementResult}
import org.slf4j.LoggerFactory


class Neo4JUtil(routePath: String, graphId: String) {

  private[this] val logger = LoggerFactory.getLogger(classOf[Neo4JUtil])

  val maxIdleSession = 20
  val driver = GraphDatabase.driver(routePath, getConfig)

  def getConfig: Config = {
    val config = Config.build
    config.withEncryptionLevel(Config.EncryptionLevel.NONE)
    config.withMaxIdleSessions(maxIdleSession)
    config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates)
    config.toConfig
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      try {
        driver.close()
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
  })

  def getNodeProperties(identifier: String): java.util.Map[String, AnyRef] = {
    val session = driver.session()
    val query = s"""MATCH (n:${graphId}{IL_UNIQUE_ID:"${identifier}"}) return n;"""
    val statementResult = session.run(query)
    statementResult.single().get("n").asMap()
  }

  def updateNodeProperty(identifier: String, key: String, value: String): Unit = {
    val query = s"""MATCH (n:$graphId {IL_UNIQUE_ID:"$identifier"}) SET n.$key=["$value"];"""
    logger.info("Query: " + query)
    val session = driver.session()
    val result = session.run(query)
    if (result.hasNext)
      logger.info("Successfully Updated node with identifier: $identifier")
    else throw new Exception(s"Unable to update the node with identifier: $identifier")
  }

  def executeQuery(query: String) = {
    val session = driver.session()
    session.run(query)
  }


}
