package org.sunbird.publish.helpers

import java.io.File

import org.slf4j.LoggerFactory
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.util.CloudStorageUtil

import scala.concurrent.ExecutionContext

trait EcarGenerator extends ObjectBundle {

	private[this] val logger = LoggerFactory.getLogger(classOf[EcarGenerator])

	def generateEcar(obj: ObjectData, pkgType: List[String])(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): Map[String, String] = {
		logger.info("Generating Ecar For : " + obj.identifier)
		val enObjects: List[Map[String, AnyRef]] = getDataForEcar(obj).getOrElse(List())
		pkgType.flatMap(pkg => Map(pkg -> generateEcar(obj, enObjects, pkg))).toMap
	}

	def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]]

	// this method returns only cloud url for given pkg
	def generateEcar(obj: ObjectData, objList: List[Map[String, AnyRef]], pkgType: String)(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): String = {
		logger.info(s"Generating ${pkgType} Ecar For : " + obj.identifier)
		val bundle: File = getObjectBundle(obj, objList, pkgType)
		uploadFile(Some(bundle), obj.identifier).getOrElse("")
	}

	private def uploadFile(fileOption: Option[File], identifier: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
		fileOption match {
			case Some(file: File) => {
				val folder = "questionset" + File.separator + identifier
				val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(true))
				logger.info(s"EcarGenerator ::: uploadFile ::: ecar url for $identifier is : ${urlArray(1)}")
				Some(urlArray(1))
			}
			case _ => None
		}
	}
}
