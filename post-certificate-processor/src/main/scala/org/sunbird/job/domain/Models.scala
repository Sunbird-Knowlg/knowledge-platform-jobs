package org.sunbird.job.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._


case class Actor(id: String, `type`: String = "User")

case class EventContext(channel: String = "in.ekstep",
                        env: String = "Course",
                        sid: String = UUID.randomUUID().toString,
                        did: String = UUID.randomUUID().toString,
                        pdata: util.Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.learning.platform", "pid" -> "course-certificate-generator").asJava,
                        cdata: Array[util.Map[String, String]])


case class EventData(props: Array[String], `type`: String, iteration: Int = 1)

case class EventObject(id: String, `type`: String, rollup: util.Map[String, String])

case class CertificateAuditEvent(eid: String = "AUDIT",
                                 ets: Long = System.currentTimeMillis(),
                                 mid: String = s"LP.AUDIT.${System.currentTimeMillis()}.${UUID.randomUUID().toString}",
                                 ver: String = "3.0",
                                 actor: Actor,
                                 context: EventContext = EventContext(
                                   cdata = Array[util.Map[String, String]]()
                                 ),
                                 `object`: EventObject,
                                 edata: EventData)



