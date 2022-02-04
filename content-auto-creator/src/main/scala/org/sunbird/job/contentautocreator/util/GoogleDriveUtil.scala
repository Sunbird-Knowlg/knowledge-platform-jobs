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
import org.sunbird.job.contentautocreator.task.ContentAutoCreatorConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util.Slug

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.charset.Charset
import java.util


object GoogleDriveUtil {
  private[this] val logger = LoggerFactory.getLogger("GoogleDriveUtil")
  val initialBackoffDelay: Int =  1200000 // 20 min
  val maximumBackoffDelay: Int = 3900000 // 65 min
  val incrementBackoffDelay: Int = 300000 // 5 min
  var currentBackOffDelay: Int = initialBackoffDelay

  private def initialize(config: ContentAutoCreatorConfig): Drive = {
    val jacksonFactory = new JacksonFactory
    val gDriveAppName = config.getString("content_auto_creator.gdrive.application_name","drive-download-sunbird")

    try {
      val httpTransport = GoogleNetHttpTransport.newTrustedTransport
      new Drive.Builder(httpTransport, jacksonFactory, getCredentials(config)).setApplicationName(gDriveAppName).build
    } catch {
      case e: Exception =>
        logger.error("GoogleDriveUtil:: Error occurred while creating google drive client ::: " + e.getMessage, e)
        throw new Exception("Error occurred while creating google drive client ::: " + e.getMessage)
    }
  }

  @throws[Exception]
  private def getCredentials(config: ContentAutoCreatorConfig): GoogleCredential = {
    val scope = util.Arrays.asList(DriveScopes.DRIVE_FILE, DriveScopes.DRIVE, DriveScopes.DRIVE_METADATA)
    val gDriveCredentials = config.getString("content_auto_creator.g_service_acct_cred","")
    val credentialsStream = new ByteArrayInputStream(gDriveCredentials.getBytes(Charset.forName("UTF-8")))
    val credential = GoogleCredential.fromStream(credentialsStream).createScoped(scope)
    credential
  }

  @throws[Exception]
  def downloadFile(fileId: String, saveDir: String, mimeType: String)(implicit config: ContentAutoCreatorConfig): File = {
    var file: File = null
      while(file == null && currentBackOffDelay <= maximumBackoffDelay) {
      try {
        val drive = initialize(config)
        logger.info("GoogleDriveUtil:: downloadFile:: drive: " + drive + " || drive.files: " + drive.files().list())
        val getFile = drive.files.get(fileId)
        logger.info("GoogleDriveUtil:: downloadFile:: getFile: " + getFile)
        getFile.setFields("id,name,size,owners,mimeType,properties,permissionIds,webContentLink")
        logger.info("GoogleDriveUtil:: downloadFile:: getFile.setFields: ")
        val googleDriveFile = getFile.execute
        logger.info("GoogleDriveUtil :: downloadFile ::: Drive File Details:: " + googleDriveFile)
        val fileName = Slug.makeSlug(googleDriveFile.getName)
        logger.info("GoogleDriveUtil :: downloadFile ::: Slug fileName :: " + fileName)
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
        file = new File(saveFilePath)
        file = Slug.createSlugFile(file)
        logger.info("GoogleDriveUtil :: downloadFile :: File Downloaded Successfully. Sluggified File Name: " + file.getAbsolutePath)
        if (null != file && (currentBackOffDelay != initialBackoffDelay)) currentBackOffDelay = initialBackoffDelay
        return file
      } catch {
        case ge: GoogleJsonResponseException => logger.error("GoogleDriveUtil :: downloadFile :: GoogleJsonResponseException :: Error Occurred while downloading file having id " + fileId + " | Error is ::" + ge.getDetails.toString, ge)
          throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid Response Received From Google API for file Id : " + fileId + " | Error is : " + ge.getDetails.toString)
        case he: HttpResponseException => logger.error("GoogleDriveUtil :: downloadFile :: HttpResponseException :: Error Occurred while downloading file having id " + fileId + " | Error is ::" + he.getContent, he)
          if (he.getStatusCode == 403) {
            if (currentBackOffDelay <= maximumBackoffDelay) delay(currentBackOffDelay)
            else if (currentBackOffDelay == 2400000) currentBackOffDelay += 1500000
            else currentBackOffDelay = currentBackOffDelay * incrementBackoffDelay
          }
          else throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid Response Received From Google API for file Id : " + fileId + " | Error is : " + he.getContent)
        case e: Exception =>
          logger.error("GoogleDriveUtil :: downloadFile :: Exception :: Error Occurred While Downloading Google Drive File having Id " + fileId + " : " + e.getMessage, e)
          if (e.isInstanceOf[ServerException]) throw e
          else throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid Response Received From Google API for file Id : " + fileId + " | Error is : " + e.getMessage)
      }
    }
    file
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
