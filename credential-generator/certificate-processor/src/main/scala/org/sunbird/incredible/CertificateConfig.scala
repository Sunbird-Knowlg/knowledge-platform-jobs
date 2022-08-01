package org.sunbird.incredible

case class CertificateConfig(basePath: String, encryptionServiceUrl: String, contextUrl: String, evidenceUrl: String,
                             issuerUrl: String, signatoryExtension: String, accessCodeLength: Double = 6)


case class StorageParams(cloudStorageType: String, azureStorageKey: String, azureStorageSecret: String, azureContainerName: String,
                         awsStorageKey: Option[String] = None, awsStorageSecret: Option[String] = None, awsContainerName: Option[String] =None,
                         cephs3StorageKey: Option[String] = None, cephs3StorageSecret: Option[String] = None, cephs3ContainerName: Option[String] =None)