package org.sunbird.job.knowlg.publish.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.slf4j.LoggerFactory
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JanusGraphUtil, ScalaJsonUtil}

import java.util
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Core selection logic for Dynamic Assess, per docs/dynamic-assess-questionset-schema.md.
 *
 * `skill` and `difficultyTarget` are read straight off the QuestionSet's own node
 * properties (RefreshBodyHelper.parseDifficultyRate-style), `minCriteria`/`multiplier`
 * come from job config, never from the request.
 *
 * The pool query filters on the QSet's declared framework's resolved deepest-category
 * `code` (e.g. "topic" for BMGS, "skill" for USF), not a hardcoded "skill" field, per
 * "Resolving which category 'skill' actually is, at runtime" in the schema doc.
 */
object DynamicAssessHelper {
  case class Allocation(skill: String, difficulty: String, requiredCount: Int)
  case class SkillDifficultyResult(skill: String, difficulty: String, selectedIds: List[String])
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

  /**
   * Minimum-first, availability-aware allocation.
   * 1. Reserve minCriteria per skill (split across difficulty buckets proportional to the shared target).
   * 2. Distribute the remainder across skills weighted by each skill's candidate availability,
   *    without exceeding the shared E/M/D totals.
   */
  def allocate(skills: List[String], difficultyTarget: Map[String, Int], minCriteria: Int,
               availability: (String, String) => Int): List[Allocation] = {

    val remainingByDifficulty = scala.collection.mutable.Map(difficultyLevels.map(l => l -> difficultyTarget.getOrElse(l, 0)): _*)
    val allocated = scala.collection.mutable.Map[(String, String), Int]().withDefaultValue(0)

    // Step 1: reserve minCriteria per skill, spread across difficulty buckets in proportion to the shared target.
    skills.foreach { skill =>
      var toReserve = minCriteria
      difficultyLevels.foreach { level =>
        if (toReserve > 0 && remainingByDifficulty(level) > 0) {
          val take = math.min(toReserve, remainingByDifficulty(level))
          allocated((skill, level)) += take
          remainingByDifficulty(level) -= take
          toReserve -= take
        }
      }
    }

    // Step 2: distribute the remainder, weighted by candidate availability per skill x difficulty.
    difficultyLevels.foreach { level =>
      var remainder = remainingByDifficulty(level)
      if (remainder > 0) {
        val availabilityBySkill = skills.map(s => s -> math.max(0, availability(s, level) - allocated((s, level)))).toMap
        val totalAvailable = availabilityBySkill.values.sum
        if (totalAvailable > 0) {
          // Ratios computed against the level's fixed starting remainder, not the live/shrinking
          // one — otherwise later skills in the list get systematically under-allocated relative
          // to earlier ones even with identical availability, since each share would be a fraction
          // of an already-reduced number instead of a true proportional split.
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

    allocated.collect { case ((skill, level), count) if count > 0 => Allocation(skill, level, count) }.toList
  }

  /** Availability count only, for the allocation step, not the final fetch. */
  def getAvailabilityCount(skill: String, difficulty: String, channel: String, categoryField: String): Int = {
    searchQuestionPool(skill, difficulty, channel, categoryField, limit = 0)._1
  }

  /** Fetches up to requiredCount x multiplier candidates for one skill x difficulty bucket, then randomly picks requiredCount of them. */
  def selectForAllocation(allocation: Allocation, channel: String, categoryField: String): SkillDifficultyResult = {
    val fetchLimit = allocation.requiredCount * config.dynamicAssessMultiplier
    val (_, candidates) = searchQuestionPool(allocation.skill, allocation.difficulty, channel, categoryField, limit = fetchLimit)
    val selected = Random.shuffle(candidates).take(allocation.requiredCount)
    SkillDifficultyResult(allocation.skill, allocation.difficulty, selected)
  }

  /**
   * Resolves the QSet's declared framework's deepest (highest-indexed) category `code` — e.g.
   * "topic" for BMGS, "skill" for USF — via GET /framework/v3/read/:identifier. This is the field
   * name the pool query actually filters on; `skill` values themselves (term identifiers/names)
   * are unaffected, only which underlying Question field they're matched against changes.
   * Falls back to the literal "skill" field on any failure (blank framework, HTTP error, missing/
   * empty categories) so a framework-lookup outage degrades to the old behavior rather than
   * failing selection outright.
   */
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

  private def searchQuestionPool(skill: String, difficulty: String, channel: String, categoryField: String, limit: Int): (Int, List[String]) = {
    try {
      val filters = new util.HashMap[String, AnyRef]() {
        put("status", new util.ArrayList[String]() {{ add("Live") }})
        put("objectType", "Question")
        put(categoryField, new util.ArrayList[String]() {{ add(skill) }})
        put("difficultyLevel", difficultyLevelName.getOrElse(difficulty, difficulty))
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

  /**
   * PATCH /questionset/v5/add is a merge, not a replace (HierarchyManager.restructureUnit only
   * swaps out children sharing an id with the incoming list; anything else already attached is
   * preserved). So a refresh must remove the QSet's existing children first (removeQuestionsFromSet)
   * or a second refresh would pile the new selection on top of the old one instead of replacing it.
   * Request shape confirmed against HierarchyManager.validateRequest: rootId (identifier) + children
   * (question ids) — not the identifier/questions shape this used to send.
   */
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

  /** One read per identifier, mirroring the fmps fork's getItemsByIdentifiers. Returns full Question metadata (body, options, etc.), not just identifiers. */
  def getQuestionsByIdentifiers(identifiers: List[String]): List[Map[String, AnyRef]] = {
    identifiers.flatMap { identifier =>
      try {
        val response = httpUtil.get(config.questionReadURL + identifier)
        if (response.isSuccess) {
          val body = ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
          val result = body.getOrElse("result", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
          result.get("question").map(_.asInstanceOf[Map[String, AnyRef]])
        } else {
          logger.warn(s"DynamicAssessHelper :: getQuestionsByIdentifiers failed to fetch $identifier, status=${response.status}")
          None
        }
      } catch {
        case e: Exception =>
          logger.error(s"DynamicAssessHelper :: getQuestionsByIdentifiers exception fetching $identifier", e)
          None
      }
    }
  }

  /**
   * Builds a normal publish-request event (action="publish") for the given Content identifier, in the same
   * envelope shape this job's own input events use (see EventFixture). Emitted back onto the job's own input
   * topic after a Dynamic Assess ECML refresh, so the refreshed body (already written to Cassandra by
   * updateContentBody) goes through the real ContentPublishFunction path and gets ECAR regeneration +
   * versioning, instead of only ever existing as a raw Cassandra write that offline/ECAR consumers never see.
   */
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
