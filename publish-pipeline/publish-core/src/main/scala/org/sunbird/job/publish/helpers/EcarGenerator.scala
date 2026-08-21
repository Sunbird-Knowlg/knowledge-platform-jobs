package org.sunbird.job.publish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.util.{CloudStorageUtil, JanusGraphUtil}

import java.io.File
import java.nio.file.Files
import java.security.MessageDigest
import scala.concurrent.ExecutionContext

case class EcarResult(urls: Map[String, String], artifactHash: Option[String] = None, prevArtifactHash: Option[String] = None) {
	// staged for saveOnSuccess to persist alongside the rest of the metadata, rather than written to the graph immediately
	def hashMeta: Map[String, AnyRef] = artifactHash.map(hash => Map[String, AnyRef]("artifactHash" -> hash, "prevArtifactHash" -> prevArtifactHash.orNull)).getOrElse(Map.empty)
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

	// returns the cloud url for the given pkg, plus (newHash, prevHash) when this pkgType's bundle included the object's own artifact
	def generateEcar(obj: ObjectData, objList: List[Map[String, AnyRef]], pkgType: String)(implicit ec: ExecutionContext, janusGraphUtil: JanusGraphUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig): (String, Option[(String, Option[String])]) = {
		logger.info(s"Generating ${pkgType} Ecar For : " + obj.identifier)
		val (bundle, artifactFile) = getObjectBundle(obj, objList, pkgType)
		val hashInfo = artifactFile.flatMap(file => computeArtifactHash(obj, file))
		val url = uploadFile(Some(bundle), obj.identifier, obj.dbObjType.replaceAll("Image", "")).getOrElse("")
		(url, hashInfo)
	}

	// computed hash is not persisted here — the caller stages it into the ObjectData's metadata
	// so it is written by saveOnSuccess only once the publish is actually confirmed
	protected def computeArtifactHash(obj: ObjectData, artifactFile: File)(implicit janusGraphUtil: JanusGraphUtil): Option[(String, Option[String])] = {
		try {
			val newHash = sha256Hex(artifactFile)
			val currentProps = Option(janusGraphUtil.getNodeProperties(obj.identifier))
			val prevHash = currentProps.flatMap(props => Option(props.get("artifactHash"))).map(_.toString)
			Some((newHash, prevHash))
		} catch {
			case e: Exception =>
				logger.error(s"EcarGenerator ::: Unable to compute artifactHash for ${obj.identifier}: ${e.getMessage}", e)
				None
		}
	}

	private def sha256Hex(file: File): String = {
		val digest = MessageDigest.getInstance("SHA-256")
		val buffer = new Array[Byte](8192)
		val input = Files.newInputStream(file.toPath)
		try {
			var bytesRead = input.read(buffer)
			while (bytesRead != -1) {
				digest.update(buffer, 0, bytesRead)
				bytesRead = input.read(buffer)
			}
		} finally {
			input.close()
		}
		digest.digest().map("%02x".format(_)).mkString
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
