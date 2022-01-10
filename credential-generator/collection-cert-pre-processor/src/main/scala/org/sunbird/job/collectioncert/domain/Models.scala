package org.sunbird.job.collectioncert.domain

import java.util.{Date, UUID}

case class EnrolledUser(userId: String, oldId: String = null, issuedOn: Date = null, additionalProps: Map[String, Any] = Map[String, Any]())

case class AssessedUser(userId: String, additionalProps: Map[String, Any] = Map[String, Any]())


case class ActorObject(id: String = "Certificate Generator", `type`: String = "System")

case class EventContext(pdata: Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.learning.platform"))


case class EventObject(id: String, `type`: String = "GenerateCertificate")

case class BEJobRequestEvent(actor: ActorObject= ActorObject(),
                          eid: String = "BE_JOB_REQUEST",
                          edata: Map[String, AnyRef],
                          ets: Long = System.currentTimeMillis(),
                          context: EventContext = EventContext(),
                          mid: String = s"LMS.${UUID.randomUUID().toString}",
                          `object`: EventObject
                         )

case class AssessmentUserAttempt(contentId: String, score: Double, totalScore: Double)