package org.sunbird.job.publish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.util.{CloudStorageUtil, Neo4JUtil}

import java.io.File
import scala.concurrent.ExecutionContext

trait EcarGenerator extends ObjectBundle {

	private[this] val logger = LoggerFactory.getLogger(classOf[EcarGenerator])

	def generateEcar(obj: ObjectData, pkgType: List[String])(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig): Map[String, String] = {
		logger.info("Generating Ecar For : " + obj.identifier)
		val enObjects: List[Map[String, AnyRef]] = getDataForEcar(obj).getOrElse(List())
		pkgType.flatMap(pkg => Map(pkg -> generateEcar(obj, enObjects, pkg))).toMap
	}

	def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]]

	// this method returns only cloud url for given pkg
	def generateEcar(obj: ObjectData, objList: List[Map[String, AnyRef]], pkgType: String)(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig): String = {
		logger.info(s"Generating ${pkgType} Ecar For : " + obj.identifier)
		val bundle: File = getObjectBundle(obj, objList, pkgType)
		uploadFile(Some(bundle), obj.identifier, obj.dbObjType.replaceAll("Image", "")).getOrElse("")
	}

	private def uploadFile(fileOption: Option[File], identifier: String, objectType: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
		fileOption match {
			case Some(file: File) => {
				logger.info("bundle file path ::: "+file.getAbsolutePath)
				val folder = objectType.toLowerCase + File.separator + identifier
				val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(false))
				logger.info(s"EcarGenerator ::: uploadFile ::: ecar url for $identifier is : ${urlArray(1)}")
				Some(urlArray(1))
			}
			case _ => None
		}
	}
}
