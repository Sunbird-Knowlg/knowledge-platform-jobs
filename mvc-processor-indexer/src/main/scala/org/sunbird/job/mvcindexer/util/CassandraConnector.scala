package org.sunbird.job.mvcindexer.util

import org.slf4j.LoggerFactory
import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import org.apache.commons.lang3.StringUtils
import org.sunbird.common.Platform
import org.sunbird.jobs.samza.util.JobLogger
import java.net.InetSocketAddress
//import java.util._


object CassandraConnector {
  private[this] lazy val logger = LoggerFactory.getLogger(CassandraConnector.getClass)
  val table = "content_data"
  var session = null

//  def getSession: Nothing = {
//    if (session != null) {
//      logger.info("CassandraSession Exists")
//      return session
//    }
//    val serverIP = Platform.config.getString("cassandra.lp.connection")
//    logger.info("Cassandra keyspace is " + Platform.config.getString("cassandra.keyspace"))
//    if (serverIP == null) logger.info("Server ip of cassandra is null")
//    logger.info("Server ip of cassandra is " + serverIP)
//    val connectionInfo = util.Arrays.asList(serverIP.split(","))
//    val addressList = getSocketAddress(connectionInfo)
//    val cluster = Cluster.builder.addContactPointsWithPorts(addressList).build
//    session = cluster.connect(Platform.config.getString("cassandra.keyspace"))
//    logger.info("The server IP " + serverIP + "\n Session created " + session)
//    session
//  }

  def updateContentProperties(contentId: String, map: Map[String, AnyRef]): Unit = {
    val session = getSession
    if (null == map || map.isEmpty) return
    val query = getUpdateQuery(map.keySet)
    if (query == null) return
    val ps = session.prepare(query)
    val values = new Array[AnyRef](map.size + 1)
    try {
      var i = 0
      import scala.collection.JavaConversions._
      for (entry <- map.entrySet) {
        if (null != entry.getValue) {
          values(i) = entry.getValue
          i += 1
        }
      }
      values(i) = contentId
      val bound:BoundStatement = ps.bind(values)
      logger.info("Executing the statement to insert into cassandra for identifier  " + contentId)
      session.execute(bound)
    } catch {
      case e: Nothing =>
        System.out.println("Exception " + e)
        logger.info("Exception while inserting data into cassandra " + e)
    }
  }

  private def getUpdateQuery(properties: Set[String]): String = {
    val sb = new StringBuilder
    if (null != properties && !properties.isEmpty) {
      sb.append("UPDATE " + table + " SET last_updated_on = dateOf(now()), ")
      val updateFields = new StringBuilder
      for (property <- properties) {
        if (StringUtils.isBlank(property.asInstanceOf[CharSequence])) return null
        updateFields.append(property.asInstanceOf[String].trim).append(" = ?, ")
      }
      sb.append(StringUtils.removeEnd(updateFields.toString, ", "))
      sb.append(" where content_id = ?")
    }
    sb.toString
  }

//  private def getSocketAddress(hosts: List[String]) = {
//    val connectionList = new Nothing
//    import scala.collection.JavaConversions._
//    for (connection <- hosts) {
//      val conn = connection.split(":")
//      val host = conn(0)
//      val port = Integer.valueOf(conn(1))
//      connectionList.add(new Nothing(host, port))
//    }
//    connectionList
//  }
}
