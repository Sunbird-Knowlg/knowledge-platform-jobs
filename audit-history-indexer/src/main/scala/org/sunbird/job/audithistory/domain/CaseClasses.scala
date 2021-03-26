package org.sunbird.job.audithistory.domain

import java.io.Serializable
import java.util.Date

@SerialVersionUID(-5779950964487302125L)
case class AuditHistoryRecord(var objectId: String, objectType: String, label: String, graphId: String, var userId: String, requestId: String, logRecord: String, operation: String, createdOn: Date, summary: String) extends Serializable