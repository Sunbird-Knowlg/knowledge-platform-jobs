package org.sunbird.kp.flink.functions

import java.lang.reflect.Type
import java.util

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder, Select}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.async.core.cache.{DataCache, RedisConnect}
import org.sunbird.async.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.async.core.util.CassandraUtil
import org.sunbird.kp.flink.task.RelationCacheUpdaterConfig

import scala.collection.JavaConverters._


class RelationCacheUpdater(config: RelationCacheUpdaterConfig)
                          (implicit val stringTypeInfo: TypeInformation[String],
                           @transient var cassandraUtil: CassandraUtil = null)
    extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {

    private[this] val logger = LoggerFactory.getLogger(classOf[RelationCacheUpdater])
    val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
    private var dataCache: DataCache = _
    lazy private val gson = new Gson()
    lazy private val mapper: ObjectMapper = new ObjectMapper()


    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
        dataCache = new DataCache(config, new RedisConnect(config), config.leafNodesStore, List())
        dataCache.init()
    }

    override def close(): Unit = {
        super.close()
    }

    override def processElement(event: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
        metrics.incCounter(config.successEventCount)
        metrics.incCounter(config.totalEventsCount)
        println("Got event:" + event)
        logger.info("Got event logger: " + event)
        val edata = event.get("edata").asInstanceOf[java.util.Map[String, AnyRef]]
        println("processElement:: " + edata)
        if (isValidEvent(edata)) {
            val hierarchy = getCassandraHierarchy(edata.get("id").asInstanceOf[String])
            println("processElement:: " + hierarchy)
            val leafNodeMap = populateLeafNodes(edata.get("id").asInstanceOf[String], hierarchy)
            val ancestorsMap = populateAncenstors(edata.get("id").asInstanceOf[String], hierarchy)
            storeDataInCache(leafNodeMap, ancestorsMap)
        }

    }

    override def metricsList(): List[String] = {
        List(config.successEventCount, config.failedEventCount, config.totalEventsCount)
    }

    private def isValidEvent(edata: java.util.Map[String, AnyRef]): Boolean = {
        println("isValidEvent:: " + edata.get("mimeType") + edata.get("id") + edata.get("action"))
        val isTrue = StringUtils.equalsIgnoreCase(edata.getOrDefault("action", "").asInstanceOf[String], "publish-shallow-content") &&
            StringUtils.equalsIgnoreCase(edata.getOrDefault("mimeType", "").asInstanceOf[String], "application/vnd.ekstep.content-collection") &&
            StringUtils.isNotBlank(edata.getOrDefault("id", "").asInstanceOf[String])
        println("isValidEvent:: " + isTrue)
        isTrue
    }

    private def getCassandraHierarchy(identifier: String): java.util.Map[String, AnyRef] = {
        val row: Row = readHierarchyFromDb(identifier, List("hierarchy"))
        val hierarchy = row.getObject("hierarchy").asInstanceOf[String]
        println("getCassandraHierarchy:: " + hierarchy)
        mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]])
    }

    private def populateLeafNodes(identifier: String, hierarchy: java.util.Map[String, AnyRef]): Map[String, AnyRef] = {
        val leafNodeMap: Map[String, AnyRef] = Map(identifier -> hierarchy.get("leafNodes").asInstanceOf[Set[String]])
        recursiveLeafNodes(hierarchy.getOrDefault("children",
            new util.ArrayList[java.util.Map[String, AnyRef]]())
            .asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]], leafNodeMap)
        println("populateLeafNodes:: " + leafNodeMap.keys)
        leafNodeMap
    }

    private def recursiveLeafNodes(children: java.util.List[java.util.Map[String, AnyRef]], leafNodeMap: Map[String, AnyRef]): Unit = {
        children.forEach(child => {
            if (StringUtils.equalsIgnoreCase(child.get("mimeType").asInstanceOf[String], "application/vnd.ekstep.content-collection")) {
                val updatedLeadNodeMap = leafNodeMap ++ Map(child.get("identifier").asInstanceOf[String]
                    -> child.getOrDefault("leafNodes", new util.ArrayList[String]()).asInstanceOf[java.util.List[String]])
                if (StringUtils.equalsIgnoreCase(child.get("visibility").asInstanceOf[String], "Parent"))
                    recursiveLeafNodes(child.getOrDefault("children",
                        new util.ArrayList[java.util.Map[String, AnyRef]]())
                        .asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]], updatedLeadNodeMap)
            }
        })
    }

    private def populateAncenstors(identifier: String, hierarchy: java.util.Map[String, AnyRef]): java.util.Map[String, java.util.Set[String]] = {
        val ancestorsMap: java.util.Map[String, java.util.Set[String]] = new java.util.HashMap[String, java.util.Set[String]]()
        val ancestors: java.util.Set[String] = new java.util.HashSet[String]()
        getRecursiveAncenstors(identifier, hierarchy, ancestorsMap, ancestors, true)
        println("populateAncenstors:: " + ancestorsMap.keySet())
        ancestorsMap
    }

    private def getRecursiveAncenstors(identifier: String, hierarchy: java.util.Map[String, AnyRef], ancestorsMap: java.util.Map[String, java.util.Set[String]], ancestors: java.util.Set[String], flag: Boolean): Unit = {
        val mimeType = hierarchy.get("mimeType").asInstanceOf[String]
        if (!StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection"))
            ancestorsMap.put(identifier, ancestors)
        else if (CollectionUtils.isNotEmpty(hierarchy.get("children").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]])) {
            if (flag) ancestors.add(identifier)
            val newFlag = !(StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection") && StringUtils.equalsIgnoreCase(hierarchy.get("visibility").asInstanceOf[String], "Default"))
            hierarchy.get("children").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
                .forEach(child => {
                    getRecursiveAncenstors(child.get("identifier").asInstanceOf[String], child, ancestorsMap, ancestors, newFlag)
                })
            ancestors.remove(identifier)
        }
    }

    private def storeDataInCache(leafNodesMap: Map[String, AnyRef], ancestorMap: java.util.Map[String, java.util.Set[String]]) = {
        leafNodesMap.foreach(each => dataCache.addList(each._1, each._2.asInstanceOf[List[String]]))
        ancestorMap.entrySet().forEach(each => dataCache.addList(each.getKey, each.getValue.asScala.toList))
    }

    def readHierarchyFromDb(identifier: String, properties: List[String]): Row = {
        val select = QueryBuilder.select()
        if (properties.nonEmpty) {
            properties.foreach(prop => select.column(prop).as(prop))
        }
        println("readHierarchyFromDb:: " + config.dbKeyspace + "." + config.dbTable)
        val selectQuery = select.from(config.dbKeyspace, config.dbTable)
        val clause: Clause = QueryBuilder.eq(config.hierarchyPrimaryKey.head, identifier)
        selectQuery.where.and(clause)
        val selectWhere: Select.Where = QueryBuilder.select().all()
            .from(config.dbKeyspace, config.dbTable).where()
        val row = cassandraUtil.find(selectWhere.toString).asScala.toList.head
        println("readHierarchyFromDb:: " + row.getColumnDefinitions)
        row
    }
}
