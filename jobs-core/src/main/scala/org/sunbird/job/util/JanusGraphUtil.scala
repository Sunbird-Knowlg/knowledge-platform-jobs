package org.sunbird.job.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.janusgraph.core.{JanusGraph, JanusGraphFactory}

import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig

import java.util
import scala.collection.JavaConverters._

class JanusGraphUtil(config: BaseJobConfig) extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[JanusGraphUtil])
  private val graphId = "domain" // Assuming 'domain' as default
  private val LOG_IDENTIFIER = "learning_graph_events"

  @transient private var _graph: JanusGraph = null

  private def graph: JanusGraph = {
    if (_graph == null) {
      synchronized {
        if (_graph == null) {
          val storageHost = config.getString("janusgraph.storage.host", "localhost")
          val storagePort = config.getString("janusgraph.storage.port", "9042")
          val storageBackend = config.getString("janusgraph.storage.backend", "cql")
          val keyspace = config.getString("janusgraph.storage.keyspace", "janusgraph")

          val map = new java.util.HashMap[String, AnyRef]()
          map.put("storage.backend", storageBackend)
          map.put("storage.hostname", storageHost)
          map.put("storage.port", storagePort)
          map.put("storage.cql.keyspace", keyspace)
          map.put("storage.cql.read-consistency-level", config.getString("janusgraph.storage.cql.read-consistency-level", "ONE"))
          map.put("storage.cql.write-consistency-level", config.getString("janusgraph.storage.cql.write-consistency-level", "ONE"))
          map.put("storage.cql.local-datacenter", config.getString("janusgraph.storage.cql.local-datacenter", "datacenter1"))
          map.put("log.learning_graph_events.backend", config.getString("janusgraph.log.learning_graph_events.backend", "default"))

          logger.info(s"Initializing Direct JanusGraph Instance. Storage: $storageHost:$storagePort, Keyspace: $keyspace")
          val conf = new org.apache.commons.configuration2.MapConfiguration(map)
          _graph = JanusGraphFactory.open(conf)
        }
      }
    }
    _graph
  }

  val isrRelativePathEnabled = config.getBoolean("cloudstorage.metadata.replace_absolute_path", false)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      try {
        if (null != _graph) _graph.close()
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
  })

  def getNodeProperties(identifier: String): java.util.Map[String, AnyRef] = {
    val tx = graph.buildTransaction().start()
    try {
      val traversal = tx.traversal().V().has("IL_UNIQUE_ID", identifier).elementMap()
      val result: java.util.Map[AnyRef, AnyRef] = if (traversal.hasNext) traversal.next().asInstanceOf[java.util.Map[AnyRef, AnyRef]] else null
      if (result != null) {
        val map = new util.HashMap[String, AnyRef]()
        result.asScala.foreach {
          case (k: String, v: AnyRef) if v != null => map.put(k, v)
          case _ =>
        }
        if (isrRelativePathEnabled) CSPMetaUtil.updateAbsolutePath(map)(config) else map
      } else null
    } catch {
      case e: Exception =>
        logger.error(s"Error fetching node properties for $identifier", e)
        null
    } finally {
      tx.rollback()
    }
  }

  def getNodePropertiesWithObjectType(objectType: String, limit: Int = 100): util.List[util.Map[String, AnyRef]] = {
    val tx = graph.buildTransaction().start()
    try {
        val traversal = tx.traversal().V().has("IL_FUNC_OBJECT_TYPE", objectType).has("IL_SYS_NODE_TYPE", "DATA_NODE").limit(limit).elementMap()
        val result = new util.ArrayList[util.Map[String, AnyRef]]()
        while(traversal.hasNext) {
             val item = traversal.next().asInstanceOf[java.util.Map[AnyRef, AnyRef]]
             val map = new util.HashMap[String, AnyRef]()
             item.asScala.foreach {
               case (k: String, v: AnyRef) if v != null => map.put(k, v)
               case _ =>
             }
             result.add(map)
        }
      if (isrRelativePathEnabled) CSPMetaUtil.updateAbsolutePath(result)(config) else result
    } catch {
      case e: Exception =>
        logger.error(s"Error fetching properties for objectType $objectType", e)
        null
    } finally {
      tx.rollback()
    }
  }

  def getNodesName(identifiers: List[String]): Map[String, String] = {
    val tx = graph.buildTransaction().start()
    try {
        val g = tx.traversal()
        val result = scala.collection.mutable.Map[String, String]()
        identifiers.foreach { identifier =>
          try {
            val traversal = g.V().has("IL_UNIQUE_ID", identifier).values[String]("name")
            if (traversal.hasNext) {
              result.put(identifier, traversal.next())
            } else {
              result.put(identifier, "")
            }
          } catch {
            case e: Exception => result.put(identifier, "")
          }
        }
        result.toMap
    } catch {
      case e: Exception =>
        logger.error(s"Error fetching node names for ${identifiers.mkString(",")}", e)
        Map()
    } finally {
      tx.rollback()
    }
  }

  def updateNodeProperty(identifier: String, key: String, value: String): Unit = {
    val tx = graph.buildTransaction().logIdentifier(LOG_IDENTIFIER).start()
    val g = tx.traversal()
    try {
      val vertexTraversal = g.V().has("IL_UNIQUE_ID", identifier)
      if (vertexTraversal.hasNext) {
        vertexTraversal.next().property(key, value)
        tx.commit()
        logger.info(s"Successfully Updated node with identifier: $identifier")
      } else {
        tx.rollback()
        logger.error(s"Unable to update the node with identifier: $identifier. Node not found.")
        throw new Exception(s"Unable to update the node with identifier: $identifier. Node not found.")
      }
    } catch {
      case e: Exception =>
        if (tx.isOpen) tx.rollback()
        logger.error(s"Unable to update the node with identifier: $identifier. Error: ${e.getMessage}", e)
        throw new Exception(s"Unable to update the node with identifier: $identifier. Error: ${e.getMessage}", e)
    }
  }
  
  def executeQuery(query: String) = {
      logger.error("executeQuery is NOT SUPPORTED in JanusGraphUtil. Please refactor to use typed methods.")
      throw new UnsupportedOperationException("executeQuery is NOT SUPPORTED in JanusGraphUtil")
  }

  def updateNode(identifier: String, metadata: Map[String, AnyRef]): Unit = {
    val updatedMetadata = if (isrRelativePathEnabled) CSPMetaUtil.updateRelativePath(metadata.asJava)(config) else metadata.asJava
    logger.info(s"Metadata for updating identifier : ${identifier} is : ${updatedMetadata}")
    val tx = graph.buildTransaction().logIdentifier(LOG_IDENTIFIER).start()
    val g = tx.traversal()
    try {
      val vertexTraversal = g.V().has("IL_UNIQUE_ID", identifier)
      if (vertexTraversal.hasNext) {
        val vertex = vertexTraversal.next()
        updatedMetadata.asScala.foreach { case (k, v) =>
          if (v != null) {
            g.V(vertex.id()).properties(k).drop().iterate()
            if (v.isInstanceOf[util.List[_]]) {
              for (item <- v.asInstanceOf[util.List[_]].asScala if item != null)
                vertex.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.list, k, item)
            } else vertex.property(k, v)
          }
        }
        tx.commit()
        logger.info(s"Successfully Updated node with identifier: $identifier")
      } else {
        tx.rollback()
        logger.error(s"Unable to update the node with identifier: $identifier. Node not found.")
        throw new Exception(s"Unable to update the node with identifier: $identifier. Node not found.")
      }
    } catch {
      case e: Exception =>
        if (tx.isOpen) tx.rollback()
        logger.error(s"Unable to update the node with identifier: $identifier. Error: ${e.getMessage}", e)
        throw new Exception(s"Unable to update the node with identifier: $identifier. Error: ${e.getMessage}", e)
    }
  }

  def deleteNode(identifier: String): Unit = {
    val tx = graph.buildTransaction().logIdentifier(LOG_IDENTIFIER).start()
    val g = tx.traversal()
    try {
      val vertexTraversal = g.V().has("IL_UNIQUE_ID", identifier)
      if (vertexTraversal.hasNext) {
        vertexTraversal.next().remove()
        tx.commit()
        logger.info(s"Successfully Deleted node with identifier: $identifier")
      } else {
        tx.rollback()
        logger.warn(s"Unable to delete the node with identifier: $identifier. Node not found.")
      }
    } catch {
      case e: Exception =>
        if (tx.isOpen) tx.rollback()
        logger.error(s"Unable to delete the node with identifier: $identifier. Error: ${e.getMessage}", e)
        throw new Exception(s"Unable to delete the node with identifier: $identifier. Error: ${e.getMessage}", e)
    }
  }

}
