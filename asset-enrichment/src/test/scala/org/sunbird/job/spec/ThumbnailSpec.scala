package org.sunbird.job.spec

import java.io.File
import org.sunbird.spec.BaseTestSpec
import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.job.task.AssetEnrichmentConfig
import org.sunbird.job.util.{FileUtils, ThumbnailUtil}

class ThubnailUtilSpec extends BaseTestSpec {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig = new AssetEnrichmentConfig(config)

  "ThumbnailUtil.generateOutFile" should " return null for no file" in {
    val file = new ThumbnailUtilTest().generateOutFile(null, 150)
    file should be(None)
  }

  "ThumbnailUtil.generateOutFile" should " return None for the file" in {
    val imageUrl = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113233717480390656195/artifact/bitcoin-4_1545114579639.jpg"
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
    val originalURL = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/kp_ft_1563562323128/artifact/sample_1563562323191.mp4"
    try {
      val originalFile = FileUtils.copyURLToFile(contentId, originalURL, originalURL.substring(originalURL.lastIndexOf("/") + 1, originalURL.length))
      val result = new ThumbnailUtilTest().generateOutFile(originalFile.get, 150)
      result should be(None)
    } finally {
      FileUtils.deleteDirectory(new File(s"/tmp/${contentId}"))
    }
  }
}


class ThumbnailUtilTest extends ThumbnailUtil {

}