package org.sunbird.job.util

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig

import java.util

class CassandraUtil(host: String, port: Int, config: BaseJobConfig) {

  private[this] val logger = LoggerFactory.getLogger("CassandraUtil")

  val isrRelativePathEnabled = config.getBoolean("cloudstorage.metadata.replace_absolute_path", false)

  val cluster = {
    Cluster.builder()
      .addContactPoints(host)
      .withPort(port)
      .withoutJMXReporting()
      .build()
  }
  var session = cluster.connect()

  def findOne(query: String): Row = {
    try {
      val rs: ResultSet = session.execute(query)
      val result = rs.one
      result
    } catch {
      case ex: DriverException =>
        logger.error(s"findOne - Error while executing query $query :: ", ex)
        this.reconnect()
        this.findOne(query)
    }
  }

  def find(query: String): util.List[Row] = {
    try {
      val rs: ResultSet = session.execute(query)
      rs.all
    } catch {
      case ex: DriverException =>
        this.reconnect()
        this.find(query)
    }
  }

  def upsert(query: String, urlReplaceReq: Boolean = false): Boolean = {
    logger.info("cassandra util ::: urlReplaceReq:: " + urlReplaceReq)
    val updatedQuery = if (isrRelativePathEnabled && urlReplaceReq) CSPMetaUtil.updateRelativePath(query)(config) else query
    logger.info("updated query ::: " + updatedQuery)
    val rs: ResultSet = session.execute(updatedQuery)
    rs.wasApplied
  }

  def getUDTType(keyspace: String, typeName: String): UserType = session.getCluster.getMetadata.getKeyspace(keyspace).getUserType(typeName)

  def reconnect(): Unit = {
    this.session.close()
    val cluster: Cluster = Cluster.builder.addContactPoint(host).withPort(port).build
    this.session = cluster.connect
  }

  def close(): Unit = {
    this.session.close()
  }

  def update(query: Statement): Boolean = {
    val rs: ResultSet = session.execute(query)
    rs.wasApplied
  }

  def executePreparedStatement(query: String, params: Object*): util.List[Row] = {
    val rs: ResultSet = session.execute(session.prepare(query).bind(params: _*))
    rs.all()
  }

}
