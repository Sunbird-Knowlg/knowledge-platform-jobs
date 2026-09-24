package org.sunbird.job.knowlg.publish.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.slf4j.LoggerFactory
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JanusGraphUtil, ScalaJsonUtil}

import java.util
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.Random

/** Core selection logic for Dynamic Assess (docs/dynamic-assess-questionset-schema.md); minCriteria/multiplier come from job config, never the request. */
object DynamicAssessHelper {
  case class Allocation(skill: String, difficulty: String, requiredCount: Int)
  case class SkillDifficultyResult(skill: String, difficulty: String, selectedIds: List[String])
  case class AllocationResult(allocations: List[Allocation], minCriteriaShortfalls: List[String])
}

class DynamicAssessHelper(config: KnowlgPublishConfig, httpUtil: HttpUtil) {

  import DynamicAssessHelper._

  private[this] val logger = LoggerFactory.getLogger(classOf[DynamicAssessHelper])

  private val difficultyLevels = List("E", "M", "D")
  private val difficultyLevelName = Map("E" -> "EASY", "M" -> "MEDIUM", "D" -> "DIFFICULT")

  /** Best-effort numeric coercion — handles "5", "5.0", boxed Int/Long/Double alike. */
  private def toIntSafe(v: AnyRef): Int = v match {
    case n: java.lang.Number => n.intValue()
    case s: String => scala.util.Try(s.toDouble.toInt).getOrElse(0)
    case _ => 0
  }

  /** Deduped — a repeated skill in the request would otherwise double-count toward minCriteria/feasibility. */
  def parseSkills(nodeProps: java.util.Map[String, AnyRef]): List[String] = {
    Option(nodeProps.get("skill")).map {
      case l: java.util.List[_] => l.asScala.toList.map(_.toString)
      case arr: Array[_] => arr.toList.map(_.toString)
      case s: String if s.nonEmpty => ScalaJsonUtil.deserialize[List[String]](s)
      case _ => Nil
    }.getOrElse(Nil).distinct
  }

  def parseDifficultyTarget(nodeProps: java.util.Map[String, AnyRef]): Map[String, Int] = {
    Option(nodeProps.get("difficultyTarget")).map {
      case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap.map { case (k, v) => k -> toIntSafe(v) }
      case s: String if s.nonEmpty => ScalaJsonUtil.deserialize[Map[String, Int]](s)
      case _ => Map.empty[String, Int]
    }.getOrElse(Map.empty[String, Int])
  }

  /** minCriteria x skills.length <= E+M+D. Doesn't check per-skill availability, just the shared-total math. */
  def isFeasible(skills: List[String], difficultyTarget: Map[String, Int], minCriteria: Int): Boolean = {
    val total = difficultyLevels.map(l => difficultyTarget.getOrElse(l, 0)).sum
    total > 0 && skills.nonEmpty && (minCriteria * skills.length) <= total
  }

  /** Minimum-first, availability-aware allocation; skills that can't cover their own minCriteria are reported in minCriteriaShortfalls. */
  def allocate(skills: List[String], difficultyTarget: Map[String, Int], minCriteria: Int,
               availability: (String, String) => Int): AllocationResult = {

    val remainingByDifficulty = scala.collection.mutable.Map(difficultyLevels.map(l => l -> difficultyTarget.getOrElse(l, 0)): _*)
    val allocated = scala.collection.mutable.Map[(String, String), Int]().withDefaultValue(0)
    val shortfalls = scala.collection.mutable.ListBuffer[String]()

    // Step 1: reserve minCriteria per skill, only taking from buckets that skill actually has candidates in.
    skills.foreach { skill =>
      var toReserve = minCriteria
      difficultyLevels.foreach { level =>
        if (toReserve > 0 && remainingByDifficulty(level) > 0) {
          val avail = math.max(0, availability(skill, level) - allocated((skill, level)))
          val take = math.min(toReserve, math.min(remainingByDifficulty(level), avail))
          if (take > 0) {
            allocated((skill, level)) += take
            remainingByDifficulty(level) -= take
            toReserve -= take
          }
        }
      }
      if (toReserve > 0) shortfalls += skill
    }

    // Step 2: distribute the remainder, weighted by candidate availability per skill x difficulty.
    difficultyLevels.foreach { level =>
      var remainder = remainingByDifficulty(level)
      if (remainder > 0) {
        val availabilityBySkill = skills.map(s => s -> math.max(0, availability(s, level) - allocated((s, level)))).toMap
        val totalAvailable = availabilityBySkill.values.sum
        if (totalAvailable > 0) {
          // Fixed starting remainder, not the live/shrinking one, or later skills get under-allocated.
          val startingRemainder = remainder
          skills.foreach { skill =>
            if (remainder > 0) {
              val share = math.min(remainder, math.round(startingRemainder.toDouble * availabilityBySkill(skill) / totalAvailable).toInt.max(0))
              val capped = math.min(share, availabilityBySkill(skill))
              if (capped > 0) {
                allocated((skill, level)) += capped
                remainder -= capped
              }
            }
          }
          // any leftover from rounding goes to whichever skill still has spare availability
          skills.foreach { skill =>
            if (remainder > 0) {
              val spare = math.max(0, availability(skill, level) - allocated((skill, level)))
              val take = math.min(remainder, spare)
              if (take > 0) {
                allocated((skill, level)) += take
                remainder -= take
              }
            }
          }
        }
      }
    }

    val allocations = allocated.collect { case ((skill, level), count) if count > 0 => Allocation(skill, level, count) }.toList
    AllocationResult(allocations, shortfalls.toList)
  }

  /** Availability count only, for the allocation step, not the final fetch. limit=1 keeps the response light; `count` reflects the total match regardless. */
  def getAvailabilityCount(skill: String, difficulty: String, channel: String, categoryField: String, poolObjectType: String): Int = {
    searchQuestionPool(skill, difficulty, channel, categoryField, poolObjectType, limit = 1)._1
  }

  /** Fetches up to requiredCount x multiplier candidates for one skill x difficulty bucket, then randomly picks requiredCount of them. */
  def selectForAllocation(allocation: Allocation, channel: String, categoryField: String, poolObjectType: String): SkillDifficultyResult = {
    val fetchLimit = allocation.requiredCount * config.dynamicAssessMultiplier
    val (_, candidates) = searchQuestionPool(allocation.skill, allocation.difficulty, channel, categoryField, poolObjectType, limit = fetchLimit)
    val selected = Random.shuffle(candidates).take(allocation.requiredCount)
    SkillDifficultyResult(allocation.skill, allocation.difficulty, selected)
  }

  /** Resolves the framework's deepest category `code` (e.g. "topic"/"skill") as the pool filter field; falls back to "skill" on any failure. */
  def resolveCategoryCode(framework: String): String = {
    if (framework.isEmpty) return "skill"
    try {
      val response = httpUtil.get(config.frameworkReadURL + framework)
      if (response.isSuccess) {
        val body = ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
        val result = body.getOrElse("result", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
        val fw = result.getOrElse("framework", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
        val categories = fw.getOrElse("categories", List.empty[Map[String, AnyRef]]).asInstanceOf[List[Map[String, AnyRef]]]
        if (categories.isEmpty) {
          logger.warn(s"DynamicAssessHelper :: resolveCategoryCode found no categories for framework=$framework, falling back to 'skill'")
          "skill"
        } else {
          val deepest = categories.maxBy(_.getOrElse("index", Int.box(0)).asInstanceOf[Number].intValue())
          deepest.get("code").map(_.toString).filter(_.nonEmpty).getOrElse("skill")
        }
      } else {
        logger.warn(s"DynamicAssessHelper :: resolveCategoryCode failed to read framework=$framework, status=${response.status}, falling back to 'skill'")
        "skill"
      }
    } catch {
      case e: Exception =>
        logger.error(s"DynamicAssessHelper :: resolveCategoryCode exception for framework=$framework, falling back to 'skill'", e)
        "skill"
    }
  }

  /** Question's difficulty field is the custom OCD "difficultyLevel"; AssessmentItem's is its native schema field "qlevel" (schemas/assessmentitem/1.0/schema.json). */
  private def difficultyFieldName(poolObjectType: String): String =
    if (poolObjectType == "AssessmentItem") "qlevel" else "difficultyLevel"

  private def searchQuestionPool(skill: String, difficulty: String, channel: String, categoryField: String, poolObjectType: String, limit: Int): (Int, List[String]) = {
    try {
      val filters = new util.HashMap[String, AnyRef]() {
        put("status", new util.ArrayList[String]() {{ add("Live") }})
        put("objectType", poolObjectType)
        put(categoryField, new util.ArrayList[String]() {{ add(skill) }})
        put(difficultyFieldName(poolObjectType), difficultyLevelName.getOrElse(difficulty, difficulty))
        if (channel.nonEmpty) put("channel", channel)
      }
      val reqMap = new util.HashMap[String, AnyRef]() {
        put("request", new util.HashMap[String, AnyRef]() {
          put("filters", filters)
          put("fields", new util.ArrayList[String]() {{ add("identifier") }})
          put("limit", Int.box(limit))
        })
      }
      val response = httpUtil.post(config.searchServiceURL, ScalaJsonUtil.serialize(reqMap))
      if (response.isSuccess) {
        val body = ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
        val result = body.getOrElse("result", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
        val count = result.getOrElse("count", Int.box(0)).asInstanceOf[Int]
        val items = result.getOrElse("items", List.empty[Map[String, AnyRef]]).asInstanceOf[List[Map[String, AnyRef]]]
        val ids = items.flatMap(_.get("identifier")).map(_.toString)
        (count, ids)
      } else (0, Nil)
    } catch {
      case e: Exception =>
        logger.error(s"DynamicAssessHelper :: searchQuestionPool failed for $categoryField=$skill, difficulty=$difficulty", e)
        (0, Nil)
    }
  }

  /** PATCH /questionset/v5/add merges, not replaces (HierarchyManager.restructureUnit) — pair with removeQuestionsFromSet to avoid piling up. */
  def addQuestionsToSet(questionSetId: String, questionIds: List[String]): Boolean = {
    if (questionIds.isEmpty) return true
    try {
      val reqMap = new util.HashMap[String, AnyRef]() {
        put("request", new util.HashMap[String, AnyRef]() {
          put("questionset", new util.HashMap[String, AnyRef]() {
            put("rootId", questionSetId)
            put("children", questionIds.asJava)
          })
        })
      }
      val response = httpUtil.patch(config.questionSetAddURL, ScalaJsonUtil.serialize(reqMap))
      response.isSuccess
    } catch {
      case e: Exception =>
        logger.error(s"DynamicAssessHelper :: addQuestionsToSet failed for $questionSetId", e)
        false
    }
  }

  /** Inverse of addQuestionsToSet, called first so a refresh replaces rather than accumulates. Same rootId/children shape. */
  def removeQuestionsFromSet(questionSetId: String, questionIds: List[String]): Boolean = {
    if (questionIds.isEmpty) return true
    try {
      val reqMap = new util.HashMap[String, AnyRef]() {
        put("request", new util.HashMap[String, AnyRef]() {
          put("questionset", new util.HashMap[String, AnyRef]() {
            put("rootId", questionSetId)
            put("children", questionIds.asJava)
          })
        })
      }
      val response = httpUtil.delete(config.questionSetRemoveURL, ScalaJsonUtil.serialize(reqMap), Map("Content-Type" -> "application/json"))
      response.isSuccess
    } catch {
      case e: Exception =>
        logger.error(s"DynamicAssessHelper :: removeQuestionsFromSet failed for $questionSetId", e)
        false
    }
  }

  /** Prior refresh's children, read off node metadata already fetched for this event — no extra API call. */
  def parseExistingChildren(nodeProps: java.util.Map[String, AnyRef]): List[String] = {
    Option(nodeProps.get("childNodes")).map {
      case l: java.util.List[_] => l.asScala.toList.map(_.toString)
      case arr: Array[_] => arr.toList.map(_.toString)
      case s: String if s.nonEmpty => ScalaJsonUtil.deserialize[List[String]](s)
      case _ => Nil
    }.getOrElse(Nil)
  }

  /** ECML's pool member type — hits AssessmentItem's own read endpoint/response shape. */
  def getAssessmentItemsByIdentifiers(identifiers: List[String]): List[Map[String, AnyRef]] = {
    identifiers.flatMap { identifier =>
      try {
        val response = httpUtil.get(config.assessmentItemReadURL + identifier)
        if (response.isSuccess) {
          val body = ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
          val result = body.getOrElse("result", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
          result.get("assessment_item").map(_.asInstanceOf[Map[String, AnyRef]])
        } else {
          logger.warn(s"DynamicAssessHelper :: getAssessmentItemsByIdentifiers failed to fetch $identifier, status=${response.status}")
          None
        }
      } catch {
        case e: Exception =>
          logger.error(s"DynamicAssessHelper :: getAssessmentItemsByIdentifiers exception fetching $identifier", e)
          None
      }
    }
  }

  /** Builds a publish event for the Content id, looped back onto this job's own input topic so ECAR/versioning actually update. */
  def buildRepublishEvent(identifier: String, mimeType: String, channel: String, pkgVersion: Double): String = {
    val ets = System.currentTimeMillis()
    val mid = s"LP.$ets.${UUID.randomUUID().toString}"
    val reqMap = new util.HashMap[String, AnyRef]() {
      put("eid", "BE_JOB_REQUEST")
      put("ets", Long.box(ets))
      put("mid", mid)
      put("actor", new util.HashMap[String, AnyRef]() {{ put("id", "dynamic-assess"); put("type", "System") }})
      put("context", new util.HashMap[String, AnyRef]() {{ put("channel", channel); put("pdata", new util.HashMap[String, AnyRef]() {{ put("id", "org.sunbird.platform"); put("ver", "1.0") }}) }})
      put("object", new util.HashMap[String, AnyRef]() {{ put("id", identifier) }})
      put("edata", new util.HashMap[String, AnyRef]() {
        put("action", "publish")
        put("iteration", Int.box(1))
        put("publish_type", "public")
        put("metadata", new util.HashMap[String, AnyRef]() {
          put("identifier", identifier)
          put("objectType", "Content")
          put("mimeType", mimeType)
          put("pkgVersion", Double.box(pkgVersion))
        })
      })
    }
    ScalaJsonUtil.serialize(reqMap)
  }

  /** ECML has no graph relations to maintain, so this writes the rebuilt body directly, mirroring RefreshBodyHelper.updateContentBody. */
  def updateContentBody(cassandraUtil: CassandraUtil, identifier: String, ecmlBody: String): Boolean = {
    try {
      val updateQuery = QueryBuilder.update(config.contentKeyspaceName, config.contentTableName)
        .where(QueryBuilder.eq("content_id", identifier))
        .`with`(QueryBuilder.set("body", QueryBuilder.fcall("textAsBlob", ecmlBody)))
      cassandraUtil.upsert(updateQuery.toString)
    } catch {
      case e: Exception =>
        logger.error(s"DynamicAssessHelper :: updateContentBody failed for $identifier", e)
        false
    }
  }
}
