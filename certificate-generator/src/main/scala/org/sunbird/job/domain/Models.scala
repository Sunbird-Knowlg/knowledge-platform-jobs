package org.sunbird.job.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._


case class ActorObject(id: String, `type`: String) {
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
                     iteration: Int) {
    def this() = this("", "", "", "", "", null, "", 1)
}

case class EventObject(id: String, `type`: String) {
    def this() = this("", "CourseCertificatePostProcessor")
}

case class PostCertificateProcessEvent(actor: ActorObject,
                                       eid: String,
                                       edata: EventData,
                                       ets: Long = System.currentTimeMillis(),
                                       context: EventContext = EventContext(),
                                       mid: String,
                                       `object`: EventObject) {
    def this() = this(null, "", null, System.currentTimeMillis(), null, "", null)
}

case class Certificate(id: String,
                       name: String,
                       token: String,
                       lastIssuedOn: String) {
    def this() = this("", "", "", "")
}

case class FailedEvent(errorCode: String,
                       error: String) {
    def this() = this("", "")
}

case class FailedEventMsg(jobName: String,
                          failInfo: FailedEvent) {
    def this() = this("certificate-generator", null)
}


