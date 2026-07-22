package org.sunbird.job.util

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig

import java.util

class CassandraUtil(host: String, port: Int, config: BaseJobConfig) extends Serializable {

  private[this] val logger = LoggerFactory.getLogger("CassandraUtil")

  val isrRelativePathEnabled = config.getBoolean("cloudstorage.metadata.replace_absolute_path", false)

  // Bounded reconnect-and-retry for read queries. Previously findOne/find recursed on every
  // DriverException with no cap, so a persistently failing query (bad CQL, node down, timeout)
  // recursed until StackOverflowError and took the whole task down.
  private val maxQueryRetries: Int = 3

  val cluster = {
    Cluster.builder()
      .addContactPoints(host)
      .withPort(port)
      .withoutJMXReporting()
      .build()
  }
  var session = cluster.connect()

  def findOne(query: String): Row = executeWithRetry(query, 1)(q => session.execute(q).one)

  def find(query: String): util.List[Row] = executeWithRetry(query, 1)(q => session.execute(q).all)

  // Reconnect-and-retry up to maxQueryRetries, then rethrow so the caller (Flink) can
  // dead-letter/restart instead of the previous unbounded recursion -> StackOverflowError.
  private def executeWithRetry[T](query: String, attempt: Int)(op: String => T): T = {
    try {
      op(query)
    } catch {
      case ex: DriverException =>
        if (attempt >= maxQueryRetries) {
          logger.error(s"Cassandra query failed after $maxQueryRetries attempts, giving up: $query", ex)
          throw ex
        }
        logger.error(s"Cassandra query error (attempt $attempt of $maxQueryRetries), reconnecting: $query", ex)
        this.reconnect()
        executeWithRetry(query, attempt + 1)(op)
    }
  }

  def upsert(query: String): Boolean = {
    logger.info("cassandra util ::: query:: " + query)
    val updatedQuery = if (isrRelativePathEnabled) CSPMetaUtil.updateRelativePath(query)(config) else query
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
