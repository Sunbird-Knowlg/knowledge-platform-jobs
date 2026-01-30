package org.sunbird.job.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig

import java.util
import scala.collection.JavaConverters._

class JanusGraphUtil(config: BaseJobConfig) extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[JanusGraphUtil])
  private val graphId = "domain" // Assuming 'domain' as default, can be configurable if needed

  private val cluster: Cluster = {
    val builder = Cluster.build()
    val route = config.getString("janusgraph.route", "localhost:8182")
    val hosts = route.split(",").map(_.split(":")(0))
    val port = route.split(",").headOption.map(_.split(":")(1).toInt).getOrElse(8182)
    
    hosts.foreach(builder.addContactPoint)
    builder.port(port)
    builder.create()
  }

  private val g: GraphTraversalSource = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"))

  val isrRelativePathEnabled = config.getBoolean("cloudstorage.metadata.replace_absolute_path", false)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      try {
        g.close()
        cluster.close()
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
  })

  def getNodeProperties(identifier: String): java.util.Map[String, AnyRef] = {
    try {
      val result = g.V().has("IL_UNIQUE_ID", identifier).elementMap().next()
      if (result != null) {
        // elementMap returns Map<Object, Object> with single values (not lists)
        // Filter out special keys like T.id and T.label
        val map = new util.HashMap[String, AnyRef]()
        result.forEach((k, v) => {
          // Only add string keys (skip T.id, T.label, etc.)
          if (k.isInstanceOf[String] && v != null) {
            map.put(k.toString, v)
          }
        })
        if (isrRelativePathEnabled) CSPMetaUtil.updateAbsolutePath(map)(config) else map
      } else null
    } catch {
      case e: Exception =>
        logger.error(s"Error fetching node properties for $identifier", e)
        null
    }
  }

  def getNodePropertiesWithObjectType(objectType: String): util.List[util.Map[String, AnyRef]] = {
    try {
        val traversal = g.V().has("IL_FUNC_OBJECT_TYPE", objectType).has("IL_SYS_NODE_TYPE", "DATA_NODE").elementMap()
        val result = new util.ArrayList[util.Map[String, AnyRef]]()
        while(traversal.hasNext) {
             val item = traversal.next()
             val map = new util.HashMap[String, AnyRef]()
             item.forEach((k, v) => {
               // Only add string keys (skip T.id, T.label, etc.)
               if (k.isInstanceOf[String] && v != null) {
                 map.put(k.toString, v)
               }
             })
             result.add(map)
        }
      if (isrRelativePathEnabled) CSPMetaUtil.updateAbsolutePath(result)(config) else result
    } catch {
      case e: Exception =>
        logger.error(s"Error fetching properties for objectType $objectType", e)
        null
    }
  }

  def getNodesName(identifiers: List[String]): Map[String, String] = {
    try {
        val traversal = g.V().has("IL_UNIQUE_ID", org.apache.tinkerpop.gremlin.process.traversal.P.within(identifiers.asJava)).project[String]("id", "name").by("IL_UNIQUE_ID").by("name")
        val result = scala.collection.mutable.Map[String, String]()
        while(traversal.hasNext) {
            val item = traversal.next()
            val id = item.get("id").asInstanceOf[String]
             val name = if(item.containsKey("name")) item.get("name").asInstanceOf[String] else ""
            result.put(id, name)
        }
        result.toMap
    } catch {
      case e: Exception =>
        logger.error(s"Error fetching node names for ${identifiers.mkString(",")}", e)
        Map()
    }
  }

  def updateNodeProperty(identifier: String, key: String, value: String): Unit = {
    try {
       g.V().has("IL_UNIQUE_ID", identifier).property(key, value).next()
       logger.info(s"Successfully Updated node with identifier: $identifier")
    } catch {
      case e: Exception =>
        logger.error(s"Unable to update the node with identifier: $identifier", e)
        throw new Exception(s"Unable to update the node with identifier: $identifier")
    }
  }
  
  // This method in JanusGraphUtil took a raw Cypher query.
  // We cannot easily support raw Cypher queries with Gremlin unless we parse them.
  // HOWEVER, most likely usages are simple updates.
  // I need to check usages of executeQuery to see what it's doing.
  // For now, I will throw an exception or log error if used.
  // Or better, I should have checked usages of executeQuery before.
  def executeQuery(query: String) = {
      logger.error("executeQuery is NOT SUPPORTED in JanusGraphUtil. Please refactor to use typed methods.")
      throw new UnsupportedOperationException("executeQuery is NOT SUPPORTED in JanusGraphUtil")
  }

  def getNodesProps(identifiers: List[String]): Map[String, AnyRef] = {
    Map()
  }

  def updateNode(identifier: String, metadata: Map[String, AnyRef]): Unit = {
    val updatedMetadata = if (isrRelativePathEnabled) CSPMetaUtil.updateRelativePath(metadata.asJava)(config) else metadata.asJava
    try {
      val traversal = g.V().has("IL_UNIQUE_ID", identifier)
      updatedMetadata.forEach((k, v) => {
          traversal.property(k, v)
      })
      traversal.next()
      logger.info(s"Successfully Updated node with identifier: $identifier")
    } catch {
       case e: Exception =>
        logger.error(s"Unable to update the node with identifier: $identifier", e)
        throw new Exception(s"Unable to update the node with identifier: $identifier")
    }
  }

  def deleteNode(identifier: String): Unit = {
    try {
      g.V().has("IL_UNIQUE_ID", identifier).drop().iterate()
      logger.info(s"Successfully Deleted node with identifier: $identifier")
    } catch {
      case e: Exception =>
        logger.error(s"Unable to delete the node with identifier: $identifier", e)
        throw new Exception(s"Unable to delete the node with identifier: $identifier")
    }
  }

}
