package org.sunbird.spec

import org.apache.commons.io.FileUtils
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.util.{HTTPResponse, HttpUtil, JSONUtil, ScalaJsonUtil}

import java.io.File

class HTTPUtilSpec extends FlatSpec with Matchers {

  val httpUtil = new HttpUtil

  "get" should "return success response" in {
    val resp: HTTPResponse = httpUtil.get("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png")
    assert(resp.isSuccess)
  }

  "post" should "return success response" in {
    val reqMap = new java.util.HashMap[String, AnyRef]() {
      put("request", new java.util.HashMap[String, AnyRef]() {
        put("filters", new java.util.HashMap[String, AnyRef]() {
          put("objectType", "Content")
          put("status", "Live")
        })
        put("limit", 1.asInstanceOf[AnyRef])
        put("fields", Array[String]("identifier", "name"))
      })
    }

    val resp = httpUtil.post("https://diksha.gov.in/api/content/v1/search", JSONUtil.serialize(reqMap))
    assert(resp.isSuccess)
  }

  "getSize" should "return file size" in {
    val resp: Int = httpUtil.getSize("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png")
    println(resp)
    assert(resp>0)
  }

  "downloadFile" should "download file from provided Url" in {
    val fileUrl = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png"
    val httpUtil = new HttpUtil
    val downloadPath = "/tmp/content" + File.separator + "_temp_" + System.currentTimeMillis
    val downloadedFile = httpUtil.downloadFile(fileUrl, downloadPath)
    assert(downloadedFile.exists())
    FileUtils.deleteDirectory(downloadedFile.getParentFile)
  }
}