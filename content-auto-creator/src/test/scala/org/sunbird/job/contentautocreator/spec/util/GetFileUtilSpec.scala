package org.sunbird.job.contentautocreator.spec.util


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.contentautocreator.util.GoogleDriveUtil
import org.sunbird.job.task.ContentAutoCreatorConfig
import org.sunbird.job.util.HttpUtil

import java.io.File


class GetFileUtilSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {
  val config: Config = ConfigFactory.load("test.conf")

  "httpUtil.downloadFile" should "download non-GoogleDrive Url object" in {
    val fileUrl = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png"
    val httpUtil = new HttpUtil
    val downloadPath = "/tmp/content" + File.separator + "_temp_" + System.currentTimeMillis
    httpUtil.downloadFile(fileUrl, downloadPath)
    val downloadedFolder = new File(downloadPath)
    assert(downloadedFolder.exists() && downloadedFolder.listFiles().nonEmpty)
    println("File:: " + downloadedFolder.listFiles().head)
    FileUtils.deleteDirectory(downloadedFolder)
  }

  "GoogleDriveUtil.downloadFile" should "download non-GoogleDrive Url object" in {
    val contentConfig = new ContentAutoCreatorConfig(config)
    val fileId = "1ZUSXrODwNK52pzDJZ_fuNKK9lXBzxCsS"
    val downloadPath = "/tmp/content" + File.separator + "_temp_" + System.currentTimeMillis
    val downloadedFile = GoogleDriveUtil.downloadFile(fileId, downloadPath, "image")(contentConfig)
    assert(downloadedFile.exists())
    FileUtils.deleteDirectory(downloadedFile.getParentFile)
  }

}