package org.sunbird.job.autocreatorv2.helpers

import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder}
import org.neo4j.driver.v1.StatementResult
import org.slf4j.LoggerFactory
import org.sunbird.job.autocreatorv2.model.ExtDataConfig
import org.sunbird.job.domain.`object`.ObjectDefinition
import org.sunbird.job.util.{CassandraUtil, JSONUtil, Neo4JUtil, ScalaJsonUtil}

import java.util

trait ObjectUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])

	def saveGraphData(identifier: String, data: Map[String, AnyRef], objectDef: ObjectDefinition)(implicit neo4JUtil: Neo4JUtil) = {
		val metadata = data - ("identifier", "objectType")
		val metaQuery = metaDataQuery(metadata, objectDef)
		val query = s"""MERGE (n:domain{IL_UNIQUE_ID:"$identifier"}) ON CREATE SET $metaQuery ON MATCH SET $metaQuery;"""
		logger.info("Graph Query: " + query)
		val result: StatementResult = neo4JUtil.executeQuery(query)
		if (null != result) {
			logger.info("Object graph data stored successfully for " + identifier)
		}else {
			val msg = s"""Object graph data insertion failed for $identifier"""
			logger.error(msg)
			throw new Exception(msg)
		}
	}

	def saveExternalData(identifier: String, data: Map[String, AnyRef], extDataConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val query: Insert = QueryBuilder.insertInto(extDataConfig.keyspace, extDataConfig.table)
		query.value(extDataConfig.primaryKey(0), identifier)
		data.map(d => {
			extDataConfig.propsMapping.getOrElse(d._1, "") match {
				case "blob" => query.value(d._1.toLowerCase, QueryBuilder.fcall("textAsBlob", d._2))
				case "string" => d._2 match {
					case value: String => query.value(d._1.toLowerCase, value)
					case _ => query.value(d._1.toLowerCase, JSONUtil.serialize(d._2))
				}
				case _ => query.value(d._1, d._2)
			}
		})
		logger.info(s"Saving object external data for $identifier | Query : ${query.toString}")
		val result = cassandraUtil.upsert(query.toString)
		if (result) {
			logger.info(s"Object external data saved successfully for ${identifier}")
		} else {
			val msg = s"Object External Data Insertion Failed For ${identifier}"
			logger.error(msg)
			throw new Exception(msg)
		}
	}

	def metaDataQuery(metadata: Map[String, AnyRef], objectDef: ObjectDefinition): String = {
		metadata.map(prop => {
			if (null == prop._2) s"n.${prop._1}=${prop._2}"
			else if (objectDef.objectTypeProperties.contains(prop._1)) {
				prop._2 match {
					case _: Map[String, AnyRef] =>
						val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
						s"""n.${prop._1}=${strValue}"""
					case _: util.Map[String, AnyRef] =>
						val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
						s"""n.${prop._1}=${strValue}"""
					case _: String =>
						val strValue = JSONUtil.serialize(prop._2)
						s"""n.${prop._1}=${strValue}"""
				}
			} else {
				prop._2 match {
					case _: List[String] =>
						val strValue = ScalaJsonUtil.serialize(prop._2.asInstanceOf[List[String]].distinct)
						s"""n.${prop._1}=${strValue}"""
					case _ =>
						val strValue = JSONUtil.serialize(prop._2)
						s"""n.${prop._1}=$strValue"""
				}
			}
		}).mkString(",")
	}
}



