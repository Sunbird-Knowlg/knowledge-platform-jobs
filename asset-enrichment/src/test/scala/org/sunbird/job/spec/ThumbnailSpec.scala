package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.job.assetenricment.task.AssetEnrichmentConfig
import org.sunbird.job.assetenricment.util.ThumbnailUtil
import org.sunbird.job.util.FileUtils
import org.sunbird.spec.BaseTestSpec

import java.io.File

class ThubnailUtilSpec extends BaseTestSpec {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig = new AssetEnrichmentConfig(config)
  val imagePath = config.getString("blob.input.contentImagePath")
  val videoPath = config.getString("blob.input.contentVideoPath")

  "ThumbnailUtil.generateOutFile" should " return null for no file" in {
    val file = new ThumbnailUtilTest().generateOutFile(null, 150)
    file should be(None)
  }

  "ThumbnailUtil.generateOutFile" should " return None for the file" in {
    val imageUrl = s"$imagePath"
    try {
      val file = FileUtils.copyURLToFile("do_113233717480390656195", imageUrl, imageUrl.substring(imageUrl.lastIndexOf("/") + 1))
      val newFile = new ThumbnailUtilTest().generateOutFile(file.get, 15000)
      newFile should be(None)
    } finally {
      FileUtils.deleteDirectory(new File(s"/tmp/do_113233717480390656195"))
    }
  }

  "ThumbnailUtil.generateOutFile" should " return None for file" in {
    val contentId = "do_1127129845261680641588"
    val originalURL = s"$videoPath"
    try {
      val originalFile = FileUtils.copyURLToFile(contentId, originalURL, originalURL.substring(originalURL.lastIndexOf("/") + 1, originalURL.length))
      val result = new ThumbnailUtilTest().generateOutFile(originalFile.get, 150)
      result should be(None)
    } finally {
      FileUtils.deleteDirectory(new File(s"/tmp/$contentId"))
    }
  }
}


class ThumbnailUtilTest extends ThumbnailUtil {

}