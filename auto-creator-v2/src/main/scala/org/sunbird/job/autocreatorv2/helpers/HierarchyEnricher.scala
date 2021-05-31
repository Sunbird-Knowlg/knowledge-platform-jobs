package org.sunbird.job.autocreatorv2.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.autocreatorv2.model.ObjectData
import org.sunbird.job.task.AutoCreatorV2Config

trait HierarchyEnricher {

	private[this] val logger = LoggerFactory.getLogger(classOf[HierarchyEnricher])

	def getChildren(obj: ObjectData)(implicit config: AutoCreatorV2Config): Map[String, AnyRef] = {
		val children = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		getChildrenMap(children, Map())
	}

	def getChildrenMap(children: List[Map[String, AnyRef]], childrenMap: Map[String, AnyRef])(implicit config: AutoCreatorV2Config): Map[String, AnyRef] = {
		children.flatMap(child => {
			val objType = child.getOrElse("objectType", "").asInstanceOf[String]
			val visibility = child.getOrElse("visibility", "").asInstanceOf[String]
			val updatedChildrenMap: Map[String, AnyRef] =
				if (config.nonExpandableObjects.contains(objType) && config.graphEnabledObjects.contains(objType)) {
					Map(child.getOrElse("identifier", "").asInstanceOf[String] -> getChildData(child)) ++ childrenMap
				} else childrenMap
			val nextChild: List[Map[String, AnyRef]] = if(config.expandableObjects.contains(objType) && StringUtils.equalsIgnoreCase("Parent", visibility)) child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]] else List()
			val map = getChildrenMap(nextChild, updatedChildrenMap)
			map ++ updatedChildrenMap
		}).toMap
	}

	def enrichHierarchy(obj: ObjectData, childrens: Map[String, ObjectData])(implicit config: AutoCreatorV2Config): ObjectData = {
		val data: Map[String, AnyRef] = config.cloudProps.filter(x => obj.metadata.get(x).nonEmpty).flatMap(prop => List((prop, obj.metadata.get(prop).get))).toMap
		val hierarchy: Map[String, AnyRef] = obj.hierarchy.get ++ data
		val hChild: List[Map[String, AnyRef]] = hierarchy.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		val uHierarchy = hierarchy ++ Map ("children"-> enrichChildren(hChild, childrens))
		val extData = obj.extData.getOrElse(Map()) ++ Map("hierarchy" -> uHierarchy)
		new ObjectData(obj.identifier, obj.objectType, obj.metadata, Some(extData), Some(uHierarchy))
	}

	def enrichChildren(children: List[Map[String, AnyRef]], chObjects: Map[String, ObjectData])(implicit config: AutoCreatorV2Config): List[Map[String, AnyRef]] = {
		val newChildren = children.map(element => enrichMetadata(element, chObjects))
		newChildren
	}

	//TODO: If children is expandable object, read the hierarchy and replace in actual hierarchy.
	// If Not Found, throw exception and kill the job.
	def enrichMetadata(element: Map[String, AnyRef], chObjects: Map[String, ObjectData])(implicit config: AutoCreatorV2Config): Map[String, AnyRef] = {
		if (config.expandableObjects.contains(element.getOrElse("objectType", "").asInstanceOf[String]) && StringUtils.equalsIgnoreCase(element.getOrElse("visibility", "").asInstanceOf[String], "Parent")) {
			val children: List[Map[String, AnyRef]] = element.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
			val enrichedChildren = enrichChildren(children, chObjects: Map[String, ObjectData])
			element ++ Map("children" -> enrichedChildren)
		} else if (config.nonExpandableObjects.contains(element.getOrElse("objectType", "").asInstanceOf[String])
		  && config.graphEnabledObjects.contains(element.getOrElse("objectType", "").asInstanceOf[String])
		  && chObjects.contains(element.getOrElse("identifier", "").asInstanceOf[String])) {
			element ++ getChildData(chObjects.getOrElse(element.getOrElse("identifier", "").asInstanceOf[String], "").asInstanceOf[ObjectData].metadata)
		} else element
	}

	def getChildData(data: Map[String, AnyRef])(implicit config: AutoCreatorV2Config): Map[String, AnyRef] = {
		val props = config.cloudProps ++ List("objectType")
		props.filter(x => data.get(x).nonEmpty).map(prop => (prop, data.getOrElse(prop, ""))).toMap
	}
}
