package org.sunbird.kp.course.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._


case class ActorObject(id: String, `type`: String)

case class Context(channel: String = "in.sunbird",
                   env: String = "Course",
                   sid: String = UUID.randomUUID().toString,
                   did: String = UUID.randomUUID().toString,
                   pdata: util.Map[String, String],
                   cdata: util.ArrayList[util.Map[String, AnyRef]],
                   rollup: util.Map[String, String])

case class eData(props: util.ArrayList[String], `type`: String)

case class EventObject(id: String, ver: String, `type`: String, rollup: util.Map[String, String])

case class TelemetryEvent(actor: ActorObject,
                          eid: String,
                          edata: eData,
                          ver: String = "3.0",
                          syncts: Long = System.currentTimeMillis(),
                          ets: Long = System.currentTimeMillis(),
                          context: Context = Context(
                            pdata = Map("ver" -> "3.0", "id" -> "org.sunbird.learning.platform", "pid" -> "course-progress-updater").asJava,
                            cdata = new util.ArrayList[util.Map[String, AnyRef]](),
                            rollup = new util.HashMap[String, String]()
                          ),
                          mid: String = s"LP.AUDIT.${UUID.randomUUID().toString}",
                          `object`: EventObject,
                          tags: util.List[AnyRef] = new util.ArrayList[AnyRef]()
                         )

case class Progress(activity_type: String,
                    activity_id: String,
                    context_id: String,
                    agg: Map[String, Int],
                    agg_last_updated: Map[String, Long]
                   )
