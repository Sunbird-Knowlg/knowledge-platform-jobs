package org.sunbird.job.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._


case class ActorObject(id: String = "Course Certificate Post Processor", `type`: String = "System") {
    def this() = this("Course Certificate Post Processor", "System")
}

case class EventContext(channel: String = "in.sunbird",
                        pdata: util.Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.platform").asJava) {
    def this() = this("in.sunbird", new java.util.HashMap[String, String] {{put("ver", "1.0")
    put("id", "org.sunbird.platform")}})
}


case class EventData(batchId: String,
                     userId: String,
                     courseId: String,
                     courseName: String,
                     templateId: String,
                     certificate: Certificate,
                     action: String,
                     iteration: Int)

case class EventObject(id: String, `type`: String)

case class PostCertificateProcessEvent(actor: ActorObject,
                                       eid: String,
                                       edata: EventData,
                                       ets: Long = System.currentTimeMillis(),
                                       context: EventContext = EventContext(),
                                       mid: String,
                                       `object`: EventObject)

case class Certificate(id: String,
                       name: String,
                       token: String,
                       lastIssuedOn: String)

case class FailedEvent(errorCode: String,
                       error: String)

case class FailedEventMsg(jobName: String = "certificate-generator",
                          failInfo: FailedEvent)


