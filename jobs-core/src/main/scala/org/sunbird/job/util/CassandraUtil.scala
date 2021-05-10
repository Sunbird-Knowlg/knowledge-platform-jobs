package org.sunbird.job.util

import java.util

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException

class CassandraUtil(host: String, port: Int) {


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
      rs.one
    } catch {
      case ex: DriverException =>
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

  def upsert(query: String): Boolean = {
    val rs: ResultSet = session.execute(query)
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

  def updateDownloadUrl(id: String, downloadUrl: String): Unit = {
    val query = "update dialcodes.dialcode_images set status=2, url='" + downloadUrl + "' where filename='" + id + "'"
    println("query: " + query)
    executeQuery(query)
  }

  def updateDownloadZIPUrl(id: String, downloadZIPUrl: String): Unit = {
    val query = "update dialcodes.dialcode_batch set status=2, url='" + downloadZIPUrl + "' where processid=" + id
    println("query: " + query)
    executeQuery(query)
  }

  def updateFailure(id: String, errMsg: String): Unit = {
    val query = "update dialcodes.dialcode_batch set status=3, url='' where processid=" + id
    println("query: " + query)
    executeQuery(query)
  }

  private def executeQuery(query: String): Unit = {
    session.execute(query)
  }
}
