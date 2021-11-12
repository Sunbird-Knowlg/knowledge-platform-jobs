package org.sunbird.job.recounciliation.domain

import java.util
import java.util.{Date, UUID}
import scala.collection.JavaConverters._


case class ActorObject(id: String, `type`: String = "User")

case class EventContext(channel: String = "in.sunbird",
                        env: String = "Course",
                        sid: String = UUID.randomUUID().toString,
                        did: String = UUID.randomUUID().toString,
                        pdata: util.Map[String, String] = Map("ver" -> "3.0", "id" -> "org.sunbird.learning.platform", "pid" -> "course-progress-updater").asJava,
                        cdata: Array[util.Map[String, String]])


case class EventData(props: Array[String], `type`: String)

case class EventObject(id: String, `type`: String, rollup: util.Map[String, String])

case class TelemetryEvent(actor: ActorObject,
                          eid: String = "AUDIT",
                          edata: EventData,
                          ver: String = "3.0",
                          syncts: Long = System.currentTimeMillis(),
                          ets: Long = System.currentTimeMillis(),
                          context: EventContext = EventContext(
                            cdata = Array[util.Map[String, String]]()
                          ),
                          mid: String = s"LP.AUDIT.${UUID.randomUUID().toString}",
                          `object`: EventObject,
                          tags: util.List[AnyRef] = new util.ArrayList[AnyRef]()
                         )

case class ContentStatus(contentId: String, status: Int = 0, completedCount: Int = 0, viewCount: Int = 1, fromInput: Boolean = true, eventsFor: List[String] = List())

case class UserContentConsumption(userId: String, batchId: String, courseId: String, contents: Map[String, ContentStatus])

case class UserActivityAgg(activity_type: String,
                           user_id: String,
                           activity_id: String,
                           context_id: String,
                           aggregates: Map[String, Double],
                           agg_last_updated: Map[String, Long]
                   )

case class CollectionProgress(userId: String, batchId: String, courseId: String, progress: Int, completedOn: Date, contentStatus: Map[String, Int], inputContents: List[String], completed: Boolean = false)

case class UserEnrolmentAgg(activityAgg: UserActivityAgg, collectionProgress: Option[CollectionProgress] = None)

