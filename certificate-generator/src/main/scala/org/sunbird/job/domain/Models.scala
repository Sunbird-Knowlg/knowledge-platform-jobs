package org.sunbird.job.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._


case class ActorObject(id: String = "Course Certificate Post Processor", `type`: String = "System")

case class EventContext(channel: String = "in.sunbird",
                        pdata: util.Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.platform").asJava)


case class EventData(batchId: String,
                     userId: String,
                     courseId: String,
                     courseName: String,
                     templateId: String,
                     certificate: Certificate,
                     action: String = "post-process-certificate",
                     iteration: Int = 1)

case class EventObject(id: String, `type`: String = "CourseCertificatePostProcessor")

case class PostCertificateProcessEvent(actor: ActorObject,
                                       eid: String = "BE_JOB_REQUEST",
                                       edata: EventData,
                                       ets: Long = System.currentTimeMillis(),
                                       context: EventContext = EventContext(),
                                       mid: String = s"LP.1564144562948.${UUID.randomUUID().toString}",
                                       `object`: EventObject)

case class Certificate(id: String,
                       name: String,
                       token: String,
                       lastIssuedOn: String)

case class FailedEvent(errorCode: String,
                       error: String)

case class FailedEventMsg(jobName: String = "certificate-generator",
                          failInfo: FailedEvent)


