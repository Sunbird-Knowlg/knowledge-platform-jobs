package org.sunbird.job.helpers

import java.io.File

import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CloudStorageUtil, FileUtils, ImageResizerUtil, Neo4JUtil, ScalaJsonUtil, Slug}
import org.im4java.core.Info
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.models.Asset
import org.sunbird.job.task.AssetEnrichmentConfig

import scala.collection.mutable


trait ImageEnrichmentHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[ImageEnrichmentHelper])
  private val CONTENT_FOLDER = "content"
  private val ARTIFACT_FOLDER = "artifact"

  def imageEnrichment(asset: Asset)(config: AssetEnrichmentConfig, definitionCache: DefinitionCache, cloudStorageUtil: CloudStorageUtil, neo4JUtil: Neo4JUtil): Unit = {
    val downloadUrl = asset.getFromMetaData("downloadUrl", "").asInstanceOf[String]
    try {
      val variantsMap = optimizeImage(asset.identifier, downloadUrl)(config, definitionCache, cloudStorageUtil)
      processImage(variantsMap, asset)(neo4JUtil)
    } catch {
      case e: Exception =>
        logger.error(s"Something Went Wrong While Performing Asset Enrichment operation.Content Id: ${asset.identifier}", e)
        asset.addToMetaData("processingError", e.getMessage)
        asset.addToMetaData("status", "Failed")
        neo4JUtil.updateNode(asset.identifier, asset.getMetaData)
        throw e
    }
  }

  def optimizeImage(contentId: String, originalURL: String)(config: AssetEnrichmentConfig, definitionCache: DefinitionCache, cloudStorageUtil: CloudStorageUtil): Map[String, String] = {
    val variantsMap = mutable.Map[String, String]()
    // TODO
    // get variant from the definition once updated : getVariantFromDefinition()(definitionCache, config)
    val variants = getVariant
    if (variants != null && variants.nonEmpty) {
      val originalFile = FileUtils.copyURLToFile(contentId, originalURL, originalURL.substring(originalURL.lastIndexOf("/") + 1, originalURL.length))
      try {
        originalFile match {
          case Some(file: File) => variants.foreach(variant => {
            val resolution = variant._1
            val variantValueMap = variant._2.asInstanceOf[Map[String, AnyRef]]
            val dimension = variantValueMap.getOrElse("dimensions", List[Int]()).asInstanceOf[List[Int]]
            val dpi = variantValueMap.getOrElse("dpi", 0).asInstanceOf[Int]
            if (dimension == null || dimension.size != 2) throw new Exception("Content Optimizer Error. Image Resolution/variants is not configured for content optimization.")
            if (isImageOptimizable(file, dimension(0), dimension(1))) {
              val targetResolution = getOptimalDPI(file, dpi)
              val optimisedFile = optimizeImage(file, targetResolution, dimension(0), dimension(1), resolution)
              if (null != optimisedFile && optimisedFile.exists) {
                val optimisedURLArray = upload(optimisedFile, contentId)(cloudStorageUtil)
                variantsMap.put(resolution, optimisedURLArray(1))
              }
            } else variantsMap.put(resolution, originalURL)
          })
          case _ => logger.error("ERR_INVALID_FILE_URL", s"Please Provide Valid File Url for identifier: ${contentId}!")
            throw new Exception(s"Please Provide Valid File Url for identifier : ${contentId} and URL : ${originalURL}.")
        }
      } finally {
        FileUtils.deleteDirectory(new File(s"/tmp/${contentId}"))
      }
    } else {
      logger.error("No variants found for optimization. " + contentId)
      throw new Exception(s"Failed to process identifier : ${contentId} due to no variants.")
    }
    if (variantsMap.getOrElse("medium", "").isEmpty) {
      if (originalURL.nonEmpty) variantsMap.put("medium", originalURL)
    }
    variantsMap.toMap
  }

  private def getVariant: Map[String, AnyRef] = {
    Map[String, AnyRef]("high" -> Map("dimensions" -> List[Int](1024, 1024), "dpi" -> 240),
      "medium" -> Map("dimensions" -> List[Int](512, 512), "dpi" -> 240),
      "low" -> Map("dimensions" -> List[Int](128, 128), "dpi" -> 240))
  }

  private def getVariantFromDefinition()(definitionCache: DefinitionCache, config: AssetEnrichmentConfig): Map[String, AnyRef] = {
    val version = config.schemaSupportVersionMap.getOrElse("asset", "1.0")
    val definition = definitionCache.getDefinition("Asset", version, config.definitionBasePath)
    val variants = definition.config.getOrElse("variants", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    variants
  }

  private def optimizeImage(file: File, dpi: Double, width: Int, height: Int, resolution: String): File = {
    val fileType = FileUtils.getFileType(file)
    val proc = new ImageResizerUtil
    if (proc.isApplicable(fileType)) proc.process(file, dpi, width, height, resolution) else null
  }

  def isImageOptimizable(file: File, dimensionX: Int, dimensionY: Int): Boolean = {
    val inputFileName = file.getAbsolutePath
    val imageInfo = new Info(inputFileName, false)
    val width = imageInfo.getImageWidth
    val height = imageInfo.getImageHeight
    if (dimensionX < width && dimensionY < height) true else false
  }

  def getOptimalDPI(file: File, dpi: Int): Double = {
    val inputFileName = file.getAbsolutePath
    val imageInfo = new Info(inputFileName, false)
    val resString = imageInfo.getProperty("Resolution")
    if (resString != null) {
      val res = resString.split("x")
      if (res.nonEmpty) {
        val xresd = res(0).toDouble
        if (xresd < dpi.toDouble) xresd else dpi.toDouble
      } else 0.toDouble
    } else 0.toDouble
  }

  private def processImage(variantsMap: Map[String, String], asset: Asset)(neo4JUtil: Neo4JUtil): Unit = {
    asset.addToMetaData("status", "Live")
    asset.addToMetaData("variants", ScalaJsonUtil.serialize(variantsMap))
    logger.info(s"Processed Image for identifier ${asset.identifier}. Updating metadata.")
    neo4JUtil.updateNode(asset.identifier, asset.getMetaData)
  }

  def upload(file: File, identifier: String)(cloudStorageUtil: CloudStorageUtil): Array[String] = {
    try {
      val slug = Slug.makeSlug(identifier, true)
      val folder = s"${CONTENT_FOLDER}/${slug}/${ARTIFACT_FOLDER}"
      cloudStorageUtil.uploadFile(folder, file, Some(true))
    } catch {
      case e: Exception =>
        throw new Exception(s"Error while uploading the File for identifier : ${identifier}.", e)
    }
  }

}
