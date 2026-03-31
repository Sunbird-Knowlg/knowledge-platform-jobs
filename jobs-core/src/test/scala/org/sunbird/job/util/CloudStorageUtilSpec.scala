package org.sunbird.job.util

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.cloud.storage.StorageConfig.{AuthType, StorageType}

/**
 * Unit tests for [[CloudStorageUtil.buildStorageConfig]].
 *
 * Because `buildStorageConfig` is a pure function (strings in, [[org.sunbird.cloud.storage.StorageConfig]] out,
 * no network or filesystem side-effects) every path can be exercised without mocking
 * any cloud SDK or instantiating [[CloudStorageUtil]] itself (which would require a
 * full [[org.sunbird.job.BaseJobConfig]] and a non-blank `cloud_storage_container`).
 */
class CloudStorageUtilSpec extends FlatSpec with Matchers {

  // ── storage-type mapping ────────────────────────────────────────────────

  "buildStorageConfig" should "map 'azure' to StorageType.AZURE" in {
    val cfg = CloudStorageUtil.buildStorageConfig("azure", "key", "secret", "ACCESS_KEY", "", "")
    cfg.getType shouldBe StorageType.AZURE
  }

  it should "map 'aws' to StorageType.AWS" in {
    val cfg = CloudStorageUtil.buildStorageConfig("aws", "key", "secret", "ACCESS_KEY", "", "")
    cfg.getType shouldBe StorageType.AWS
  }

  it should "map 'gcloud' to StorageType.GCLOUD" in {
    val cfg = CloudStorageUtil.buildStorageConfig("gcloud", "key", "secret", "ACCESS_KEY", "", "")
    cfg.getType shouldBe StorageType.GCLOUD
  }

  it should "map 'oci' to StorageType.OCI" in {
    val cfg = CloudStorageUtil.buildStorageConfig("oci", "key", "secret", "ACCESS_KEY", "", "")
    cfg.getType shouldBe StorageType.OCI
  }

  it should "map 'cephs3' to StorageType.CEPHS3" in {
    val cfg = CloudStorageUtil.buildStorageConfig("cephs3", "key", "secret", "ACCESS_KEY", "", "")
    cfg.getType shouldBe StorageType.CEPHS3
  }

  it should "be case-insensitive for the cloud storage type" in {
    CloudStorageUtil.buildStorageConfig("AZURE",  "k", "s", "ACCESS_KEY", "", "").getType shouldBe StorageType.AZURE
    CloudStorageUtil.buildStorageConfig("GCloud", "k", "s", "ACCESS_KEY", "", "").getType shouldBe StorageType.GCLOUD
    CloudStorageUtil.buildStorageConfig("AWS",    "k", "s", "ACCESS_KEY", "", "").getType shouldBe StorageType.AWS
  }

  it should "throw IllegalArgumentException for an unsupported type" in {
    an [IllegalArgumentException] should be thrownBy {
      CloudStorageUtil.buildStorageConfig("gcp", "key", "secret", "ACCESS_KEY", "", "")
    }
  }

  it should "throw IllegalArgumentException for any unknown type" in {
    an [IllegalArgumentException] should be thrownBy {
      CloudStorageUtil.buildStorageConfig("s3", "key", "secret", "ACCESS_KEY", "", "")
    }
  }

  // ── auth-type: storageSecret conditional injection ──────────────────────

  it should "set storageSecret and storageKey for ACCESS_KEY auth" in {
    val cfg = CloudStorageUtil.buildStorageConfig("azure", "myKey", "mySecret", "ACCESS_KEY", "", "")
    cfg.getAuthType      shouldBe AuthType.ACCESS_KEY
    cfg.getStorageKey    shouldBe "myKey"
    cfg.getStorageSecret shouldBe "mySecret"
  }

  it should "not set storageSecret for OIDC auth" in {
    val cfg = CloudStorageUtil.buildStorageConfig("azure", "myKey", "ignoredSecret", "OIDC", "", "")
    cfg.getAuthType      shouldBe AuthType.OIDC
    // SDK builder defaults the field to "" when never set — the key point is that the
    // caller-supplied secret was NOT propagated into the config.
    cfg.getStorageSecret should not be "ignoredSecret"
  }

  it should "not set storageSecret for IAM auth" in {
    val cfg = CloudStorageUtil.buildStorageConfig("aws", "myKey", "ignoredSecret", "IAM", "", "")
    cfg.getAuthType      shouldBe AuthType.IAM
    cfg.getStorageSecret should not be "ignoredSecret"
  }

  it should "not set storageSecret for IAM_ROLE auth" in {
    val cfg = CloudStorageUtil.buildStorageConfig("aws", "myKey", "ignoredSecret", "IAM_ROLE", "", "")
    cfg.getAuthType      shouldBe AuthType.IAM_ROLE
    cfg.getStorageSecret should not be "ignoredSecret"
  }

  it should "throw IllegalArgumentException for an invalid auth type" in {
    an [IllegalArgumentException] should be thrownBy {
      CloudStorageUtil.buildStorageConfig("azure", "key", "secret", "BADAUTHTYPE", "", "")
    }
  }

  // ── optional fields: endpoint and region ───────────────────────────────

  it should "leave endPoint null when not provided" in {
    val cfg = CloudStorageUtil.buildStorageConfig("aws", "key", "secret", "ACCESS_KEY", "", "")
    cfg.getEndPoint shouldBe null
  }

  it should "set endPoint when provided" in {
    val cfg = CloudStorageUtil.buildStorageConfig("cephs3", "key", "secret", "ACCESS_KEY", "http://minio:9000", "")
    cfg.getEndPoint shouldBe "http://minio:9000"
  }

  it should "leave region null when not provided" in {
    val cfg = CloudStorageUtil.buildStorageConfig("aws", "key", "secret", "ACCESS_KEY", "", "")
    cfg.getRegion shouldBe null
  }

  it should "set region when provided" in {
    val cfg = CloudStorageUtil.buildStorageConfig("aws", "key", "secret", "ACCESS_KEY", "", "us-east-1")
    cfg.getRegion shouldBe "us-east-1"
  }

  it should "set both endPoint and region when both are provided" in {
    val cfg = CloudStorageUtil.buildStorageConfig("cephs3", "key", "secret", "ACCESS_KEY", "http://minio:9000", "us-east-1")
    cfg.getEndPoint shouldBe "http://minio:9000"
    cfg.getRegion   shouldBe "us-east-1"
  }

}
