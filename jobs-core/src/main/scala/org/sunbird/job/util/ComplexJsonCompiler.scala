package org.sunbird.job.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import java.io.IOException
import java.net.URL
import scala.collection.mutable

object ComplexJsonCompiler {
  final private val objectMapper: ObjectMapper = new ObjectMapper()
  final private val definitions: java.util.List[ObjectNode] = new java.util.ArrayList[ObjectNode]()

  @throws[IOException]
  def createConsolidatedSchema(contextMapFileURL: String, contextType: String): String = {
    val entrySchema: ObjectNode = getSchemaWithResolvedParts(contextMapFileURL)
    val consolidatedSchema: ObjectNode = objectMapper.createObjectNode.setAll(entrySchema)
    val definitionsNode: ObjectNode = objectMapper.createObjectNode
    definitions.forEach(objectNode => definitionsNode.setAll(objectNode))
    val schemaMap: mutable.Map[String, AnyRef] = ScalaJsonUtil.deserialize[mutable.Map[String, AnyRef]](consolidatedSchema.toPrettyString)
    val definitionsMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](definitionsNode.toPrettyString)

    val contextTypeSchema = schemaMap(contextType).asInstanceOf[Map[String, AnyRef]]
    val updatedContextTypeSchema = contextTypeSchema.flatMap(rec => {
      val resolvedMainMap = resolveReferences(rec, schemaMap.toMap[String, AnyRef], definitionsMap)
      resolvedMainMap
    })

    ScalaJsonUtil.serialize(updatedContextTypeSchema)
  }

  @throws[IOException]
  private def getSchemaWithResolvedParts(contextMapFileURL: String): ObjectNode = {
    val jsonUrl: URL = new URL(contextMapFileURL)
    val entrySchema = objectMapper.readTree(jsonUrl.openStream()).asInstanceOf[ObjectNode]
    val definitionsNode = entrySchema.get("$defs")
    definitionsNode match {
      case node: ObjectNode =>
        definitions.add(node)
        entrySchema.remove("$defs")
      case _ =>
    }
    entrySchema
  }

  @throws[IOException]
  private def resolveReferences(record: Tuple2[String, AnyRef], consolidatedSchemaMap: Map[String, AnyRef], definitionsMap: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    if(record._1.equalsIgnoreCase("$ref")) {
      val reference = record._2
      val referenceValue = reference.toString
      if (referenceValue.startsWith("#/$defs/")) {
        val refDefinitionString = referenceValue.substring(referenceValue.indexOf("\\#/$defs/") + 9)
        val returnMap = mutable.Map.empty ++ (definitionsMap(refDefinitionString).asInstanceOf[Map[String, AnyRef]] - "@type")
        returnMap
      } else if (referenceValue.startsWith("#/")) {
        val refDefinitionString = referenceValue.substring(referenceValue.indexOf("\\#/") + 3)
        val returnMap = mutable.Map.empty ++ consolidatedSchemaMap(refDefinitionString).asInstanceOf[Map[String, AnyRef]].flatMap(rec => {
          resolveReferences(rec, consolidatedSchemaMap, definitionsMap)
        })
        returnMap
      } else mutable.Map.empty
    }
    else if(record._2.isInstanceOf[Map[String, AnyRef]])  mutable.Map(record._1 -> resolveReferences(record._2.asInstanceOf[Map[String, AnyRef]].head, consolidatedSchemaMap, definitionsMap))
    else mutable.Map(record._1 -> record._2)
  }
}