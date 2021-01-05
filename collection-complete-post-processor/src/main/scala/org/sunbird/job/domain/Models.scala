package org.sunbird.job.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

// final event structure for generate certificate
case class CertificateGenerateEvent(eid: String = "BE_JOB_REQUEST",
                                    ets: Long = System.currentTimeMillis(),
                                    mid: String = s"LMS.${UUID.randomUUID().toString}",
                                    edata: util.Map[String, AnyRef],
                                    `object`: EventObject,
                                    context: EventContext = EventContext(),
                                    actor: ActorObject = ActorObject())

case class ActorObject(`id`: String = "Certificate Generator", `type`: String = "System")

case class EventContext(pdata: util.Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.platform").asJava)

case class EventObject(id: String, `type`: String)

// user related data need to add into generate event
case class UserDetails(data: util.ArrayList[java.util.Map[String, AnyRef]], orgId: String)

case class Data(recipientName: String, recipientId: String)

case class OrgDetails(keys: util.Map[String, AnyRef])

// course and batch related data need to add into generate event
case class CourseDetails(courseName: String, tag: String)

case class Related(courseId: String, `type`: String = "course-completion", batchId: String)

// template related data need to add into generate event
case class CertTemplate(templateId: String,
                        name: String,
                        signatoryList: util.ArrayList[util.Map[String, String]],
                        issuer: util.Map[String, AnyRef],
                        criteria: util.Map[String, AnyRef],
                        svgTemplate: String)

case class CertificateData(issuedDate: String, basePath: String)

// generate event request
case class GenerateRequest(batchId: String,
                           userId: String,
                           courseId: String,
                           template: util.Map[String, AnyRef],
                           reIssue: Boolean)
