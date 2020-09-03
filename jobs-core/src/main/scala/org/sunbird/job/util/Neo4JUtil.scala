package org.sunbird.job.util

import org.neo4j.driver.v1.{Config, GraphDatabase}
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

  def close() = {
    if (driver != null)
      driver.close()
  }

  def getNodeProperties(identifier: String): java.util.Map[String, AnyRef] = {
    val session = driver.session()
    val query = s"""MATCH (n:${graphId}{IL_UNIQUE_ID:"${identifier}"}) return n;"""
    val statementResult = session.run(query)
    statementResult.single().get("n").asMap()
  }

}
