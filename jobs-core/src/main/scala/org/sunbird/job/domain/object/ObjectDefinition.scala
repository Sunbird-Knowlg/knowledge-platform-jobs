package org.sunbird.job.domain.`object`

class ObjectDefinition(val objectType: String, val version: String, val schema: Map[String, AnyRef], val config: Map[String, AnyRef]) {

  val externalProperties = if (config.isEmpty) List() else {
    val external = config.getOrElse("external", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val extProps = external.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    extProps.keys.toList
  }

  private val relationLabelsMap: Map[String, String] = if (config.isEmpty) Map[String, String]() else {
    val relations = config.getOrElse("relations", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    relations.flatMap(r => {
      val label = r._1
      val relation = r._2.asInstanceOf[Map[String, AnyRef]]
      val direction = relation.getOrElse("direction", "").asInstanceOf[String].toUpperCase
      val relType = relation.getOrElse("type", "").asInstanceOf[String]
      val objectTypes = relation.getOrElse("objects", List[String]()).asInstanceOf[List[String]]
      objectTypes.flatMap(objType => Map(relationKey(objType, direction, relType) -> label)).toMap
    })
  }

  def relationKey(objectType: String, direction: String, relationType: String): String =
    s"${direction.toUpperCase}:${objectType.toUpperCase}:${relationType.toUpperCase}"

  def relationLabel(objectType: String, direction: String, relationType: String): Option[String] =
    relationLabelsMap.get(relationKey(objectType, direction, relationType))
}
