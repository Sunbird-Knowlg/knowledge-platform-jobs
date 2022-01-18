package org.sunbird.job.contentautocreator.util

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.drive.Drive
import com.google.api.services.drive.DriveScopes
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.ServerException
import org.sunbird.job.task.ContentAutoCreatorConfig
import org.sunbird.job.util.Slug

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.charset.Charset
import java.util


class GoogleDriveUtil (implicit config: ContentAutoCreatorConfig) {
  private val JSON_FACTORY = new JacksonFactory
  private val SCOPES = util.Arrays.asList(DriveScopes.DRIVE_READONLY)
  private val APP_NAME = config.getString("auto_creator.gdrive.application_name","drive-download-sunbird")
  private val SERVICE_ACC_CRED = config.getString("auto_creator_g_service_acct_cred","")
  val INITIAL_BACKOFF_DELAY: Int =  config.getInt("auto_creator.initial_backoff_delay", 1200000) // 20 min
  val MAXIMUM_BACKOFF_DELAY: Int = config.getInt("auto_creator.maximum_backoff_delay", 3900000) // 65 min
  val INCREMENT_BACKOFF_DELAY: Int = config.getInt("auto_creator.increment_backoff_delay", 300000) // 5 min

  var BACKOFF_DELAY: Int = INITIAL_BACKOFF_DELAY
  private[this] val logger = LoggerFactory.getLogger(classOf[GoogleDriveUtil])

  private def init(): Drive = {
    try {
      val HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport
      new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials).setApplicationName(APP_NAME).build
    } catch {
      case e: Exception =>
        logger.error("Error occurred while creating google drive client ::: " + e.getMessage, e)
        e.printStackTrace()
        throw new Exception("Error occurred while creating google drive client ::: " + e.getMessage)
    }
  }

  @throws[Exception]
  private def getCredentials: GoogleCredential = {
    val credentialsStream = new ByteArrayInputStream(SERVICE_ACC_CRED.getBytes(Charset.forName("UTF-8")))
    val credential = GoogleCredential.fromStream(credentialsStream).createScoped(SCOPES)
    credential
  }

  @throws[Exception]
  def downloadFile(fileId: String, saveDir: String, mimeType: String): File = {
    try {
      val drive = init()
      val getFile = drive.files.get(fileId)
      getFile.setFields("id,name,size,owners,mimeType,properties,permissionIds,webContentLink")
      val googleDriveFile = getFile.execute
      logger.info("GoogleDriveUtil :: downloadFile ::: Drive File Details:: " + googleDriveFile)
      val fileName = googleDriveFile.getName
      val fileMimeType = googleDriveFile.getMimeType
      logger.info("GoogleDriveUtil :: downloadFile ::: Node mimeType :: " + mimeType + " | File mimeType :: " + fileMimeType)
      if (!StringUtils.equalsIgnoreCase(mimeType, "image")) validateMimeType(fileId, mimeType, fileMimeType)
      val saveFile = new File(saveDir)
      if (!saveFile.exists) saveFile.mkdirs
      val saveFilePath: String = saveDir + File.separator + fileName
      logger.info("GoogleDriveUtil :: downloadFile :: File Id :" + fileId + " | Save File Path: " + saveFilePath)
      val outputStream = new FileOutputStream(saveFilePath)
      getFile.executeMediaAndDownloadTo(outputStream)
      outputStream.close()
      var file = new File(saveFilePath)
      file = Slug.createSlugFile(file)
      logger.info("GoogleDriveUtil :: downloadFile :: File Downloaded Successfully. Sluggified File Name: " + file.getAbsolutePath)
      if (null != file && (BACKOFF_DELAY ne INITIAL_BACKOFF_DELAY)) BACKOFF_DELAY = INITIAL_BACKOFF_DELAY
      return file
    } catch {
      case ge: GoogleJsonResponseException =>
        logger.error("GoogleDriveUtil :: downloadFile :: GoogleJsonResponseException :: Error Occurred while downloading file having id " + fileId + " | Error is ::" + ge.getDetails.toString, ge)
        throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid Response Received From Google API for file Id : " + fileId + " | Error is : " + ge.getDetails.toString)
      case he: HttpResponseException =>
        logger.error("GoogleDriveUtil :: downloadFile :: HttpResponseException :: Error Occurred while downloading file having id " + fileId + " | Error is ::" + he.getContent, he)
        he.printStackTrace()
        if (he.getStatusCode eq 403) {
          if (BACKOFF_DELAY <= MAXIMUM_BACKOFF_DELAY) delay(BACKOFF_DELAY)
          if (BACKOFF_DELAY eq 2400000) BACKOFF_DELAY += 1500000
          else BACKOFF_DELAY = BACKOFF_DELAY * INCREMENT_BACKOFF_DELAY
        }
        else throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid Response Received From Google API for file Id : " + fileId + " | Error is : " + he.getContent)
      case e: Exception =>
        logger.error("GoogleDriveUtil :: downloadFile :: Exception :: Error Occurred While Downloading Google Drive File having Id " + fileId + " : " + e.getMessage, e)
        e.printStackTrace()
        if (e.isInstanceOf[ServerException]) throw e
        else throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid Response Received From Google API for file Id : " + fileId + " | Error is : " + e.getMessage)
    }
    null
  }

  private def validateMimeType(fileId: String, mimeType: String, fileMimeType: String): Unit = {
    val errMsg = "Invalid File Url! File MimeType Is Not Same As Object MimeType for File Id : " + fileId + " | File MimeType is : " + fileMimeType + " | Node MimeType is : " + mimeType
    mimeType match {
      case "application/vnd.ekstep.h5p-archive" =>
        if (!(StringUtils.equalsIgnoreCase("application/x-zip", fileMimeType) || StringUtils.equalsIgnoreCase("application/zip", fileMimeType))) throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", errMsg)
      case "application/epub" =>
        if (!StringUtils.equalsIgnoreCase("application/epub+zip", fileMimeType)) throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", errMsg)
      case "audio/mp3" =>
        if (!StringUtils.equalsIgnoreCase("audio/mpeg", fileMimeType)) throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", errMsg)
      case "application/vnd.ekstep.html-archive" =>
        if (!StringUtils.equalsIgnoreCase("application/x-zip-compressed", fileMimeType)) throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", errMsg)
      case _ =>
        if (!StringUtils.equalsIgnoreCase(mimeType, fileMimeType)) throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", errMsg)

    }
  }

  def delay(time: Int): Unit = {
    logger.info("delay is called with : " + time)
    try Thread.sleep(time)
    catch {
      case e: Exception => e.printStackTrace();
    }
  }

}
