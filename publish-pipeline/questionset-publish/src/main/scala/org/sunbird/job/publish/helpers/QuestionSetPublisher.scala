package org.sunbird.job.publish.helpers

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3
import org.sunbird.job.util.{CassandraUtil, JSONUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.publish.helpers.{ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait QuestionSetPublisher extends ObjectReader with ObjectValidator with ObjectUpdater with ObjectEnrichment {
    val extProps = List("body", "editorState", "answer", "solutions", "instructions", "hints", "media","responseDeclaration", "interactions", "identifier")

    override def getExtData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

    def validateQuestionSet(obj: ObjectData, identifier: String): List[String] = {
        val messages = ListBuffer[String]()
        if (obj.hierarchy.get.isEmpty) messages += s"""There is no hierarchy available for : $identifier"""
        messages.toList
    }

    override def getHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
        val row = getQuestionSetHierarchy(identifier, readerConfig)
        if (null != row) {
            val data = JSONUtil.deserialize[java.util.Map[String, AnyRef]](row.getString("hierarchy"))
            Option(data.asScala.toMap)
        } else Option(Map())
    }

    override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
        val rows = getQuestionsExtData(identifiers, readerConfig)(cassandraUtil).asScala
        if (rows.nonEmpty)
            Option(rows.map(row => row.getString("identifier") -> extProps.map(prop => (prop -> row.getString(prop))).toMap).toMap)
        else
            Option(Map[String, AnyRef]())
    }

    override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
        None
    }

    def getQuestionsExtData(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
        val select = QueryBuilder.select()
        extProps.foreach(prop => if(lang3.StringUtils.equals("body", prop) | lang3.StringUtils.equals("answer", prop)) select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop) else select.column(prop).as(prop))
        val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where()
        selectWhere.and(QueryBuilder.in("identifier", identifiers))
        cassandraUtil.find(selectWhere.toString)
    }


    def getQuestionSetHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
        val selectWhere: Select.Where = QueryBuilder.select().all()
            .from(readerConfig.keyspace, readerConfig.table).
            where()
        selectWhere.and(QueryBuilder.eq("identifier", identifier))
        cassandraUtil.findOne(selectWhere.toString)
    }

    def dummyFunc = (obj: ObjectData) => {}

    def getQuestions(questionSet: ObjectData, readerConfig: ExtDataConfig)
                    (implicit cassandraUtil: CassandraUtil): List[ObjectData] = {
        val hierarchy = getHierarchy(questionSet.dbId, readerConfig).getOrElse(Map())
        val childrenMaps = populateChildrenMapRecursively(hierarchy.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]], Map())
        val extMap = getExtDatas(childrenMaps.keys.toList, readerConfig)
        extMap.getOrElse(Map()).map(child => new ObjectData(child._1, childrenMaps.getOrElse(child._1, Map()).asInstanceOf[Map[String, AnyRef]],
            Some(child._2.asInstanceOf[Map[String, AnyRef]]))).toList
    }

    /**
      *
      * @param children
      * @param childrenMap
      * @return a map of id and metadata map
      */
    def populateChildrenMapRecursively(children: List[Map[String, AnyRef]], childrenMap: Map[String, AnyRef]): Map[String, AnyRef] = {
        children.flatMap(child => {
            val updatedChildrenMap: Map[String, AnyRef] =
                if (child.getOrElse("objectType", "").asInstanceOf[String].equalsIgnoreCase("Question")) {
                    Map(child.get("identifier").asInstanceOf[String] -> child) ++ childrenMap
                } else childrenMap
            val map = populateChildrenMapRecursively(child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]], updatedChildrenMap)
            map ++ updatedChildrenMap
        }).toMap
    }
}
