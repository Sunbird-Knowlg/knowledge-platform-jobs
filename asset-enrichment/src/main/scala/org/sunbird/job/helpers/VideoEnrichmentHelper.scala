package org.sunbird.job.helpers

import java.io.File
import java.util.concurrent.TimeUnit

import javax.imageio.ImageIO
import org.apache.commons.lang3.StringUtils
import org.bytedeco.javacv.{FFmpegFrameGrabber, Java2DFrameConverter}
import org.slf4j.LoggerFactory
import org.sunbird.job.models.Asset
import org.sunbird.job.task.AssetEnrichmentConfig
import org.sunbird.job.util._

import scala.collection.mutable

trait VideoEnrichmentHelper extends ThumbnailUtil {

  private[this] val logger = LoggerFactory.getLogger(classOf[ImageEnrichmentHelper])
  private val SAMPLE_THUMBNAIL_COUNT = 5
  private val CONTENT_FOLDER = "content"
  private val ARTIFACT_FOLDER = "artifact"

  def videoEnrichment(asset: Asset)(config: AssetEnrichmentConfig, youTubeUtil: YouTubeUtil, cloudStorageUtil: CloudStorageUtil, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): Unit = {
    try {
      val videoUrl = asset.artifactUrl
      if (StringUtils.isBlank(videoUrl)) {
        logger.info("Content artifactUrl is blank.")
        throw new Exception(s"PROCESSING_ERROR :: Content artifactUrl is blank for identifier: ${asset.identifier}.")
      }
      processVideo(asset, videoUrl)(youTubeUtil, cloudStorageUtil, neo4JUtil)
      pushStreamingUrlRequest(asset, videoUrl)(config, cassandraUtil)
    } catch {
      case e: Exception =>
        logger.error(s"Something Went Wrong While Performing Asset Enrichment operation. Content Id: ${asset.identifier}. ", e)
        asset.addToMetaData("processingError", e.getMessage)
        asset.addToMetaData("status", "Failed")
        neo4JUtil.updateNode(asset.identifier, asset.getMetaData)
        throw e
    }
  }

  private def processVideo(asset: Asset, videoUrl: String)(youTubeUtil: YouTubeUtil, cloudStorageUtil: CloudStorageUtil, neo4JUtil: Neo4JUtil): Unit = {
    if (StringUtils.equalsIgnoreCase("video/x-youtube", asset.getFromMetaData("mimeType", "").asInstanceOf[String])) {
      val data = youTubeUtil.getVideoInfo(videoUrl, "snippet,contentDetails", List[String]("thumbnail", "duration"))
      if (data.nonEmpty) {
        if (data.contains("thumbnail")) asset.addToMetaData("thumbnail", data("thumbnail").asInstanceOf[String])
        if (data.contains("duration")) asset.addToMetaData("duration", data("duration").asInstanceOf[String])
      } else throw new Exception(s"Failed to get data for Youtube Video Url : ${videoUrl} and identifier: ${asset.identifier}.")
    } else {
      val videoFile = FileUtils.copyURLToFile(asset.identifier, videoUrl, videoUrl.substring(videoUrl.lastIndexOf("/") + 1))
      try {
        videoFile match {
          case Some(file: File) => videoEnrichment(asset, file)(cloudStorageUtil)
          case _ => logger.error("ERR_INVALID_FILE_URL", s"Please Provide Valid File Url for identifier: ${asset.identifier}!")
            throw new Exception(s"Please Provide Valid File Url for identifier: ${asset.identifier}!")
        }
      } finally {
        FileUtils.deleteDirectory(new File(s"/tmp/${asset.identifier}"))
      }
    }
    asset.addToMetaData("status", "Live")
    neo4JUtil.updateNode(asset.identifier, asset.getMetaData)
  }

  def pushStreamingUrlRequest(asset: Asset, videoUrl: String)(config: AssetEnrichmentConfig, cassandraUtil: CassandraUtil): Unit = {
    if (config.isStreamingEnabled && config.streamableMimeType.contains(asset.getFromMetaData("mimeType", "").asInstanceOf[String])) {
      val channel = asset.getFromMetaData("channel", "").asInstanceOf[String]
      val requestMap = Map[String, AnyRef]("content_id" -> asset.identifier, "artifactUrl" -> videoUrl, "channel" -> channel)
      val requestData = ScalaJsonUtil.serialize(requestMap)
      val requestId = s"${asset.identifier}_1.0"
      val query = s"INSERT INTO ${config.streamKeyspace}.${config.streamTable}(client_key,request_id,job_id,status,request_data,location,dt_file_created,dt_first_event,dt_last_event,dt_expiration,iteration,dt_job_submitted,dt_job_processing,dt_job_completed,input_events,output_events,file_size,latency,execution_time,err_message,stage,stage_status) VALUES ('SYSTEM_LP','${requestId}','VIDEO_STREAMING','SUBMITTED','${requestData}','${videoUrl}',NULL,NULL,NULL,NULL,0,dateOf(now()),NULL,NULL,0,0,0,0,0,NULL,NULL,NULL);"
      val result = cassandraUtil.upsert(query)
      if (!result) logger.error("FAILED_TO_PUSH_STREAMING_URL", s"Failed to Push VideoUrl for Streaming for identifier = ${asset.identifier}")
    }
  }

  private def videoEnrichment(asset: Asset, videoFile: File)(cloudStorageUtil: CloudStorageUtil): Unit = {
    val frameGrabber = new FFmpegFrameGrabber(videoFile)
    frameGrabber.start()
    val videoDuration = frameGrabber.getLengthInTime
    if (videoDuration != 0) asset.addToMetaData("duration", TimeUnit.MICROSECONDS.toSeconds(videoDuration).toString)
    val numberOfFrames = frameGrabber.getLengthInFrames
    val thumbNail = fetchThumbNail(asset.identifier, numberOfFrames, frameGrabber)
    frameGrabber.stop()
    if (null != thumbNail && thumbNail.exists) {
      logger.info("Thumbnail created for Content Id: " + asset.identifier)
      val urlArray = upload(thumbNail, asset.identifier)(cloudStorageUtil)
      val thumbUrl = urlArray(1)
      asset.addToMetaData("thumbnail", thumbUrl)
    } else logger.error(s"Thumbnail could not be generated for identifier : ${asset.identifier}.")
  }

  private def fetchThumbNail(identifier: String, numberOfFrames: Long, frameGrabber: FFmpegFrameGrabber): File = {
    val converter = new Java2DFrameConverter
    var thumbnail: File = null
    var colorCount = 0
    for (i <- 1 to SAMPLE_THUMBNAIL_COUNT) {
      val inFile = FileUtils.createFile(s"${FileUtils.getBasePath(identifier)}${File.separator}${System.currentTimeMillis}.png")
      frameGrabber.setFrameNumber((numberOfFrames / SAMPLE_THUMBNAIL_COUNT).toInt * i)
      try {
        val frame = frameGrabber.grabImage()
        val bufferedImage = converter.convert(frame)
        if (null != bufferedImage) {
          ImageIO.write(bufferedImage, "png", inFile)
          val outFile = generateOutFile(inFile)
          outFile match {
            case Some(file: File) =>
              val tmpColorCount = getImageColor(file)
              if (colorCount < tmpColorCount) {
                colorCount = tmpColorCount
                thumbnail = file
              }
            case _ => logger.error(s"Thumbnail Could Not Be Generated For : ${identifier}.")
          }
        }
      } catch {
        case e: Throwable =>
          logger.error(s"Exception while generating thumbnail for identifier: ${identifier}.", e)
          throw new Exception(s"Exception while generating thumbnail for identifier: ${identifier}. Error : ${e.getMessage}")
      }
    }
    thumbnail
  }

  private def getImageColor(imagePath: File): Int = {
    val image = ImageIO.read(imagePath)
    val colorSet = mutable.Set[Int]()
    for (r <- 0 until image.getHeight) {
      for (c <- 0 until image.getWidth) {
        val colorCode = image.getRGB(c, r)
        colorSet.add(colorCode)
      }
    }
    colorSet.size
  }

  def upload(uploadedFile: File, identifier: String)(cloudStorageUtil: CloudStorageUtil): Array[String] = {
    try {
      val slug = Slug.makeSlug(identifier, true)
      val folder = s"${CONTENT_FOLDER}/${slug}/${ARTIFACT_FOLDER}"
      cloudStorageUtil.uploadFile(folder, uploadedFile, Some(true))
    } catch {
      case e: Exception =>
        throw new Exception("Error while uploading File to cloud for identifier : ${identifier}.", e)
    }
  }

}
