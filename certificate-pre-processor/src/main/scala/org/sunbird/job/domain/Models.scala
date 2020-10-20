package org.sunbird.job.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

//done
case class CertificateGenerateEvent(eid: String = "BE_JOB_REQUEST",
                                    ets: Long = System.currentTimeMillis(),
                                    mid: String = s"LMS.${UUID.randomUUID().toString}",
                                    edata: util.Map[String, AnyRef],
                                    `object`: EventObject,
                                    context: EventContext = EventContext(),
                                    actor: ActorObject = ActorObject()
                                   )

//done
case class ActorObject(`id`: String = "Certificate Generator", `type`: String = "System")

//done
case class EventContext(pdata: util.Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.platform").asJava)

//done
case class EventObject(id: String, `type`: String)

case class EventData(certificateData: CertificateData, basePath: String)

case class CertificateData(svgTemplate: String, issuedDate: String)

case class CertTemplate(name: String,
                        tag: String,
                        notifyTemplate: util.Map[String, AnyRef],
                        signatoryList: Array[util.Map[String, String]],
                        issuer: util.Map[String, String],
                        criteria: util.Map[String, String]
                       )

//done
case class UserDetails(data: util.ArrayList[Data], orgId: String)

//done
case class Data(recipientName: String, recipientId: String)

//done
case class OrgDetails(keys: util.Map[String, AnyRef])

//done
case class OldId(oldId: String)

//done
case class CourseDetails(courseName: String)

//done
case class Related(courseId: String, `type`: String = "course-completion", batchId: String)
