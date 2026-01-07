package org.sunbird.job.content.publish.domain

case class PublishMetadata(eventContext: Map[String, AnyRef], identifier: String, objectType: String, mimeType: String, pkgVersion: Double, publishType: String, lastPublishedBy: String, schemaVersion: String = "1.0")