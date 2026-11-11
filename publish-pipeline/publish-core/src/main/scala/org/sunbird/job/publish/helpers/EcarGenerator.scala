package org.sunbird.job.publish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.util.{CloudStorageUtil, JanusGraphUtil}

import java.io.File
import scala.concurrent.ExecutionContext

case class EcarResult(urls: Map[String, String], artifactHash: Option[String] = None, prevArtifactHash: Option[String] = None) {
	def hashMeta: Map[String, AnyRef] = artifactHash.map(hash => Map[String, AnyRef]("artifactHash" -> hash) ++ prevArtifactHash.map(prev => Map[String, AnyRef]("prevArtifactHash" -> prev)).getOrElse(Map.empty)).getOrElse(Map.empty)
}

trait EcarGenerator extends ObjectBundle {

	private[this] val logger = LoggerFactory.getLogger(classOf[EcarGenerator])

	def generateEcar(obj: ObjectData, pkgType: List[String])(implicit ec: ExecutionContext, janusGraphUtil: JanusGraphUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig): EcarResult = {
		logger.info("Generating Ecar For : " + obj.identifier)
		val enObjects: List[Map[String, AnyRef]] = getDataForEcar(obj).getOrElse(List())
		var artifactHash: Option[String] = None
		var prevArtifactHash: Option[String] = None
		val urls = pkgType.map(pkg => {
			val (url, hashInfo) = generateEcar(obj, enObjects, pkg)
			hashInfo.foreach { case (hash, prevHash) =>
				artifactHash = Some(hash)
				prevArtifactHash = prevHash
			}
			pkg -> url
		}).toMap
		EcarResult(urls, artifactHash, prevArtifactHash)
	}

	def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]]

	// this method returns only cloud url for given pkg
	def generateEcar(obj: ObjectData, objList: List[Map[String, AnyRef]], pkgType: String)(implicit ec: ExecutionContext, janusGraphUtil: JanusGraphUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig): (String, Option[(String, Option[String])]) = {
		logger.info(s"Generating ${pkgType} Ecar For : " + obj.identifier)
		val (bundle, artifactHash) = getObjectBundle(obj, objList, pkgType)
		val hashInfo = artifactHash.map(hash => (hash, readPrevArtifactHash(obj)))
		val url = uploadFile(Some(bundle), obj.identifier, obj.dbObjType.replaceAll("Image", "")).getOrElse("")
		(url, hashInfo)
	}

	protected def readPrevArtifactHash(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil): Option[String] = {
		try {
			Option(janusGraphUtil.getNodeProperties(obj.identifier)).flatMap(props => Option(props.get("artifactHash"))).map(_.toString)
		} catch {
			case e: Exception =>
				logger.error(s"EcarGenerator ::: Unable to read previous artifactHash for ${obj.identifier}: ${e.getMessage}", e)
				None
		}
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
