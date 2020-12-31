package org.sunbird.incredible

case class CertificateConfig(basePath: String, encryptionServiceUrl: String, contextUrl: String, evidenceUrl: String,
                             issuerUrl: String, signatoryExtension: String, accessCodeLength: Double = 6)
