package org.sunbird.spec

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.util.FileUtils

import java.io.File

class FileUtilsSpec extends FlatSpec with Matchers {

  "getBasePath with empty identifier" should "return the path" in {
    val result = FileUtils.getBasePath("")
    result.nonEmpty shouldBe (true)
  }

  ignore should " download the media source file starting with http or https " in {
    val fileUrl: String = "https://preprodall.blob.core.windows.net/ntp-content-preprod/content/do_21273718766395392014320/artifact/book-image_1554832478631.jpg"
    val downloadedFile: File = FileUtils.downloadFile(fileUrl, "/tmp/contentBundle")
    assert(downloadedFile.exists())
  }
}
