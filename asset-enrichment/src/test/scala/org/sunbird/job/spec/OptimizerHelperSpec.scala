package org.sunbird.job.spec

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.ArgumentMatchers.{anyBoolean, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, when}
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.ImageEnrichmentFunction
import org.sunbird.job.models.Asset
import org.sunbird.job.task.AssetEnrichmentConfig
import org.sunbird.job.util.{CloudStorageUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

class OptimizerHelperSpec extends BaseTestSpec {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig = new AssetEnrichmentConfig(config)
  implicit val mockCloudUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())

  "replaceArtifactUrl" should " update the provided asset properties " in {
    doNothing().when(mockCloudUtil).copyObjectsByPrefix(anyString(), anyString(), anyBoolean())
    val asset = getAsset(EventFixture.IMAGE_ASSET, getMetaData)
    new ImageEnrichmentFunction(jobConfig).replaceArtifactUrl(asset)(mockCloudUtil)
    asset.get("artifactUrl", "") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg")
    asset.get("downloadUrl", "") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg")
    asset.get("cloudStorageKey", "") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg")
    asset.get("s3Key", "") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg")
  }

  "replaceArtifactUrl" should " throw exception for invalid asset " in {
    when(mockCloudUtil.copyObjectsByPrefix(anyString(), anyString(), anyBoolean())).thenThrow(new IllegalArgumentException("Failed to copy the artifact."))
    val asset = getAsset(EventFixture.IMAGE_ASSET, getMetaData)
    assertThrows[Exception] {
      new ImageEnrichmentFunction(jobConfig).replaceArtifactUrl(asset)(mockCloudUtil)
    }
  }


  def getAsset(event: String, metaData: Map[String, AnyRef]): Asset = {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](event)
    val asset = Asset(eventMap)
    asset.putAll(metaData)
    asset
  }

  def getMetaData: Map[String, AnyRef] = {
    val metaData = Map[String, AnyRef]("cloudStorageKey" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/tmp/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg",
      "s3Key" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/tmp/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg",
      "artifactBasePath" -> "tmp",
      "artifactUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/tmp/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg")
    metaData
  }
}
