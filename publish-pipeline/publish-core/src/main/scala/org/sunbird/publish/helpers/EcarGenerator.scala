package org.sunbird.publish.helpers

import java.io.File

import org.slf4j.LoggerFactory
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.util.CloudStorageUtil

import scala.concurrent.ExecutionContext

trait EcarGenerator extends ObjectBundle {

	private[this] val logger = LoggerFactory.getLogger(classOf[EcarGenerator])

	// This method returns map of pkgType and its cloud url
	def generateEcar(obj: ObjectData, pkgType: List[String])(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): Map[String, String] = {
		logger.info("Generating Ecar For : " + obj.identifier)
		// custom function will do children enrichment based on object type behaviour. generally applicable only for collections
		// in case of other than collection, customFn should return the root node enriched metadata as List[Map[String, AnyRef]]
		val enObjects: List[Map[String, AnyRef]] = getDataForEcar(obj).getOrElse(List())
		pkgType.flatMap(pkg => Map(pkg -> generateEcar(obj, enObjects, pkg))).toMap
	}

	def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]]

	// this method returns only cloud url for given pkg
	def generateEcar(obj: ObjectData, objList: List[Map[String, AnyRef]], pkgType: String)(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): String = {
		logger.info(s"Generating ${pkgType} Ecar For : " + obj.identifier)
		// get bundle file (ecar file)
		val bundle: File = getObjectBundle(obj, objList, pkgType)
		println("bundle file exist ::: "+bundle.exists())
		println("bundle file path ::: "+bundle.getAbsolutePath)
		uploadFile(Some(bundle), obj.identifier).getOrElse("")
		//TODO: upload and return the url
		//"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_113105564164997120111/1-vsa-qts-2_1603203738131_do_113105564164997120111_1.0_spine.ecar"
	}

	private def uploadFile(fileOption: Option[File], identifier: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
		fileOption match {
			case Some(file: File) => {
				val folder = "questionset" + File.separator + identifier
				val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(true))
				Some(urlArray(1))
			}
			case _ => None
		}
	}
}
