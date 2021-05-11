package org.sunbird.job.functions

import java.lang.reflect.Type
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.reflect.TypeToken
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.domain.Event
import org.sunbird.job.task.RelationCacheUpdaterConfig
import org.sunbird.job.util.CassandraUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._


class RelationCacheUpdater(config: RelationCacheUpdaterConfig)
                          (implicit val stringTypeInfo: TypeInformation[String],
                           @transient var cassandraUtil: CassandraUtil = null)
    extends BaseProcessFunction[Event, String](config) {

    private[this] val logger = LoggerFactory.getLogger(classOf[RelationCacheUpdater])
    private var dataCache: DataCache = _
    private var collectionCache: DataCache = _
    lazy private val mapper: ObjectMapper = new ObjectMapper()
    private val allowedActions = List("post-publish-process", "relation-cache-update")


    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)

        // Using LP cache for leafnodes, ancestors cache for the collection.
        val lpCacheConnect = new RedisConnect(config)
        dataCache = new DataCache(config, lpCacheConnect, config.relationCacheStore, List())
        dataCache.init()

        // Using DP cache to save the collection metadata cache to existing DP redis cache.
        // This job pushes only visibility: Parent data to redis.
        val dpCacheConnect = new RedisConnect(config, Option(config.dpRedisHost), Option(config.dpRedisPort))
        collectionCache = new DataCache(config, dpCacheConnect, config.collectionCacheStore, List())
        collectionCache.init()
    }

    override def close(): Unit = {
        cassandraUtil.close()
        dataCache.close()
        collectionCache.close()
        super.close()
    }

    override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
        if (event.isValidEvent(allowedActions)) {
            val rootId = event.identifier
            println("Processing - identifier: " + rootId)
            val hierarchy = getHierarchy(rootId)(metrics)
            if (MapUtils.isNotEmpty(hierarchy)) {
                val leafNodesMap = getLeafNodes(rootId, hierarchy)
                logger.info("Leaf-nodes cache updating for: " + leafNodesMap.size)
                storeDataInCache(rootId, "leafnodes", leafNodesMap, dataCache)(metrics)
                val ancestorsMap = getAncestors(rootId, hierarchy)
                logger.info("Ancestors cache updating for: "+ ancestorsMap.size)
                storeDataInCache(rootId, "ancestors", ancestorsMap, dataCache)(metrics)
                val unitsMap = getUnitMaps(hierarchy)
                logger.info("Units cache updating for: "+ unitsMap.size)
                storeDataInCache("", "", unitsMap, collectionCache)(metrics)
                metrics.incCounter(config.successEventCount)
            } else {
                logger.warn("Hierarchy Empty: " + rootId)
                metrics.incCounter(config.skippedEventCount)
            }
        } else {
            metrics.incCounter(config.skippedEventCount)
        }
        metrics.incCounter(config.totalEventsCount)
    }

    override def metricsList(): List[String] = {
        List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount, config.dbReadCount, config.cacheWrite)
    }

    private def getHierarchy(identifier: String)(implicit metrics: Metrics): java.util.Map[String, AnyRef] = {
        val hierarchy = readHierarchyFromDb(identifier)
        metrics.incCounter(config.dbReadCount)
        if (StringUtils.isNotBlank(hierarchy))
            mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]])
        else new java.util.HashMap[String, AnyRef]()
    }

    private def getLeafNodes(identifier: String, hierarchy: java.util.Map[String, AnyRef]): Map[String, List[String]] = {
        val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
        val leafNodesMap = if (StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection")) {
            val leafNodes = getOrComposeLeafNodes(hierarchy, false)
            val map: Map[String, List[String]] = if (leafNodes.nonEmpty) Map() + (identifier -> leafNodes) else Map()
            val children = getChildren(hierarchy)
            val childLeafNodesMap = if (CollectionUtils.isNotEmpty(children)) {
                children.asScala.map(child => {
                    val childId = child.get("identifier").asInstanceOf[String]
                    getLeafNodes(childId, child)
                }).flatten.toMap
            } else Map()
            map ++ childLeafNodesMap
        } else Map()
        leafNodesMap.filter(m => m._2.nonEmpty).toMap
    }

    private def getOrComposeLeafNodes(hierarchy: java.util.Map[String, AnyRef], compose: Boolean = true): List[String] = {
        if (hierarchy.containsKey("leafNodes") && !compose)
            hierarchy.getOrDefault("leafNodes", java.util.Arrays.asList()).asInstanceOf[java.util.List[String]].asScala.toList
        else {
            val children = getChildren(hierarchy)
            val childCollections = children.asScala.filter(c => isCollection(c))
            val leafList = childCollections.map(coll => getOrComposeLeafNodes(coll, true)).flatten.toList
            val ids = children.asScala.filterNot(c => isCollection(c)).map(c => c.getOrDefault("identifier", "").asInstanceOf[String]).filter(id => StringUtils.isNotBlank(id))
            leafList ++ ids
        }
    }

    private def isCollection(content: java.util.Map[String, AnyRef]): Boolean = {
        StringUtils.equalsIgnoreCase(content.getOrDefault("mimeType", "").asInstanceOf[String], "application/vnd.ekstep.content-collection")
    }

    private def getAncestors(identifier: String, hierarchy: java.util.Map[String, AnyRef], parents: List[String] = List()): Map[String, List[String]] = {
        val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
        val isCollection = (StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection"))
        val ancestors = if (isCollection) identifier :: parents else parents
        val ancestorsMap = if (isCollection) {
            getChildren(hierarchy).asScala.map(child => {
                val childId = child.get("identifier").asInstanceOf[String]
                getAncestors(childId, child, ancestors)
            }).filter(m => m.nonEmpty).reduceOption((a,b) => {
                // Here we are merging the Resource ancestors where it is used multiple times - Functional.
                // convert maps to seq, to keep duplicate keys and concat then group by key - Code explanation.
                val grouped = (a.toSeq ++ b.toSeq).groupBy(_._1)
                grouped.mapValues(_.map(_._2).toList.flatten.distinct)
            }).getOrElse(Map())
        } else {
            Map(identifier -> parents)
        }
        ancestorsMap.filter(m => m._2.nonEmpty)
    }

    private def getChildren(hierarchy: java.util.Map[String, AnyRef]) = {
        val children = hierarchy.getOrDefault("children", java.util.Arrays.asList()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        if (CollectionUtils.isEmpty(children)) List().asJava else children
    }

    private def storeDataInCache(rootId: String, suffix: String, dataMap: Map[String, AnyRef], cache: DataCache)(implicit metrics: Metrics) = {
        val finalSuffix = if (StringUtils.isNotBlank(suffix)) ":" + suffix else ""
        val finalPrefix = if (StringUtils.isNoneBlank(rootId)) rootId + ":" else ""
        try {
            dataMap.foreach(each => each._2 match {
                case value: List[String] =>
                    cache.createListWithRetry(finalPrefix + each._1 + finalSuffix, each._2.asInstanceOf[List[String]])
                    metrics.incCounter(config.cacheWrite)
                case _ =>
                    cache.setWithRetry(finalPrefix + each._1 + finalSuffix, each._2.asInstanceOf[String])
                    metrics.incCounter(config.cacheWrite)
            })
        } catch {
            case e: Throwable => {
                metrics.incCounter(config.failedEventCount)
                logger.info("Failed to write data for " + suffix + ": " + rootId + " with map: " + dataMap)
                throw e
            }
        }
    }

    def readHierarchyFromDb(identifier: String): String = {
        val columnName = "hierarchy"
        val selectQuery = QueryBuilder.select().column(columnName).from(config.dbKeyspace, config.dbTable)
        selectQuery.where.and(QueryBuilder.eq(config.hierarchyPrimaryKey.head, identifier))
        val rows = cassandraUtil.find(selectQuery.toString)
        if (CollectionUtils.isNotEmpty(rows))
            rows.asScala.head.getObject("hierarchy").asInstanceOf[String]
        else
            ""
    }

    private def getUnitMaps(hierarchy: java.util.Map[String, AnyRef]): Map[String, String] = {
        val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
        if (StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection")) {
            val children = getChildren(hierarchy).asScala
            // TODO - Here the collection with "visibility:Parent" constructed with children in it. May be we should remove children.
            if (children.nonEmpty)
                (if (StringUtils.equalsIgnoreCase(hierarchy.getOrDefault("visibility", "").asInstanceOf[String], "Parent"))
                    Map(hierarchy.get("identifier").asInstanceOf[String] -> mapper.writeValueAsString(hierarchy))
                else Map()) ++ children.flatMap(child => getUnitMaps(child)).toMap
            else Map()
        } else Map()
    }
}
