package org.sunbird.kp.course.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._


case class ActorObject(id: String, `type`: String)

case class Context(channel: String = "in.sunbird",
                   env: String = "knowledge-platform",
                   sid: String = UUID.randomUUID().toString,
                   did: String = UUID.randomUUID().toString,
                   pdata: util.Map[String, String],
                   cdata: util.ArrayList[util.Map[String, AnyRef]],
                   rollup: util.Map[String, String])

case class eData(`type`: String)

case class EventObject(id: String, ver: String, `type`: String, rollup: util.Map[String, String])

case class TelemetryEvent(actor: ActorObject = ActorObject("sunbird-telemetry", "knowledge-platform"),
                          eid: String,
                          edata: eData = eData(`type` = "platform"),
                          ver: String = "3.0",
                          syncts: Long = System.currentTimeMillis(),
                          ets: Long = System.currentTimeMillis(),
                          context: Context = Context(
                            pdata = Map("ver" -> "3.0", "pid" -> "course-aggregator").asJava,
                            cdata = new util.ArrayList[util.Map[String, AnyRef]](),
                            rollup = new util.HashMap[String, String]()
                          ),
                          mid: String,
                          `object`: EventObject = EventObject(UUID.randomUUID().toString, "3.0", "kp-telemetry-events", new util.HashMap[String, String]()),
                          tags: util.List[AnyRef] = new util.ArrayList[AnyRef]()
                         )

case class Progress(batchId: String,
                    userId: String,
                    courseId: String,
                    status: Int,
                    completedOn: Option[Long],
                    contentStatus: Map[String, Int],
                    progress: Int, completionPercentage: Int
                   )

