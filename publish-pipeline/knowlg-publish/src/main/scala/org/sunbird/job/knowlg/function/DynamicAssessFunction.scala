package org.sunbird.job.knowlg.function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.knowlg.publish.domain.Event
import org.sunbird.job.knowlg.publish.helpers.{DynamicAssessHelper, ECMLBodyBuilder}
import org.sunbird.job.knowlg.publish.helpers.DynamicAssessHelper.Allocation
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JanusGraphUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

/**
 * Dynamic Assess selection, per docs/dynamic-assess-questionset-schema.md.
 *
 * Triggered by `edata.action = "refresh-body"` on the existing publish topic (matches fmps's
 * existing action convention, ContentConstants.REFRESH_BODY, already emitted by
 * POST /content/v3/refresh/body/:identifier onto this same topic)
 * (see PublishEventRouter). Reads `skill`/`difficultyTarget` off the object's own node
 * properties, `minCriteria`/`multiplier` from job config, allocates the shared E/M/D
 * total across skills (minimum-first, availability-aware), selects questions per
 * skill x difficulty bucket, then writes the result back.
 *
 * Write-back differs by object type, these are not the same operation:
 *  - QuestionSet: already exists, this is an add (POST /questionset/v5/add).
 *  - Content (ECML): the questions get embedded into the `body` field, a direct
 *    Cassandra upsert, mirroring the fmps fork's RefreshBodyHelper.updateContentBody.
 *
 * Per-skill x difficulty failures (insufficient pool) go to the DLQ but don't stop the
 * other buckets from being processed, unlike the fmps fork's refresh-body flow, which
 * skips the whole event on any shortfall.
 */
class DynamicAssessFunction(config: KnowlgPublishConfig, httpUtil: HttpUtil,
                            @transient var janusGraphUtil: JanusGraphUtil = null,
                            @transient var cassandraUtil: CassandraUtil = null,
                            @transient var dynamicAssessHelper: DynamicAssessHelper = null)
                           (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DynamicAssessFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    janusGraphUtil = new JanusGraphUtil(config)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    dynamicAssessHelper = new DynamicAssessHelper(config, httpUtil)
  }

  override def close(): Unit = {
    if (cassandraUtil != null) cassandraUtil.close()
    super.close()
  }

  override def metricsList(): List[String] =
    List(config.dynamicAssessEventCount, config.dynamicAssessSuccessCount, config.dynamicAssessFailedCount, config.dynamicAssessSkippedCount)

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    val objectId = event.identifier
    metrics.incCounter(config.dynamicAssessEventCount)
    logger.info(s"DynamicAssessFunction :: processing $objectId, objectType=${event.objectType}")

    if (event.objectType != "QuestionSet" && event.objectType != "Content") {
      logger.info(s"DynamicAssessFunction :: unsupported objectType=${event.objectType}, skipping $objectId")
      metrics.incCounter(config.dynamicAssessSkippedCount)
      return
    }

    try {
      val nodeProps = Option(janusGraphUtil.getNodeProperties(objectId))
        .getOrElse(throw new RuntimeException(s"Node not found in JanusGraph: $objectId"))

      val skills = dynamicAssessHelper.parseSkills(nodeProps)
      val difficultyTarget = dynamicAssessHelper.parseDifficultyTarget(nodeProps)
      val channel = Option(nodeProps.get("channel")).map(_.toString).getOrElse("")
      val name = Option(nodeProps.get("name")).map(_.toString).getOrElse(objectId)
      val framework = Option(nodeProps.get("framework")).map(_.toString).getOrElse("")
      val minCriteria = config.dynamicAssessMinCriteria

      if (!dynamicAssessHelper.isFeasible(skills, difficultyTarget, minCriteria)) {
        logger.info(s"DynamicAssessFunction :: feasibility check failed for $objectId, skills=$skills, difficultyTarget=$difficultyTarget, minCriteria=$minCriteria")
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-feasibility",
          "error" -> "minCriteria x skills.length exceeds the shared E/M/D total")))
        return
      }

      // Resolved once per event: the QSet's declared framework's deepest-category code
      // (e.g. "topic" for BMGS, "skill" for USF) is the field the pool query actually filters on.
      val categoryField = dynamicAssessHelper.resolveCategoryCode(framework)

      // Step 1: availability counts per skill x difficulty, used for allocation only.
      val availability: (String, String) => Int = (skill, difficulty) =>
        dynamicAssessHelper.getAvailabilityCount(skill, difficulty, channel, categoryField)

      // Step 2: minimum-first, availability-aware allocation, determines the required count per bucket.
      val allocations = dynamicAssessHelper.allocate(skills, difficultyTarget, minCriteria, availability)

      // Step 3: for each resulting allocation, fetch required x multiplier candidates and select.
      val results = allocations.map(dynamicAssessHelper.selectForAllocation(_, channel, categoryField))

      val (fulfilled, shortfallResults): (List[(DynamicAssessHelper.SkillDifficultyResult, Allocation)], List[(DynamicAssessHelper.SkillDifficultyResult, Allocation)]) =
        results.zip(allocations).partition { case (result, allocation) => result.selectedIds.size >= allocation.requiredCount }

      shortfallResults.foreach { case (result, allocation) =>
        logger.info(s"DynamicAssessFunction :: shortfall for $objectId, skill=${allocation.skill}, difficulty=${allocation.difficulty}, required=${allocation.requiredCount}, got=${result.selectedIds.size}")
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-selection",
          "skill" -> allocation.skill, "difficulty" -> allocation.difficulty,
          "required" -> allocation.requiredCount, "available" -> result.selectedIds.size)))
      }

      val allSelectedIds = fulfilled.map(_._1).flatMap(_.selectedIds).distinct

      if (allSelectedIds.isEmpty) {
        metrics.incCounter(config.dynamicAssessFailedCount)
        logger.info(s"DynamicAssessFunction :: no questions selected for $objectId, all buckets shortfell")
        return
      }

      val written = event.objectType match {
        case "QuestionSet" =>
          // /questionset/v5/add merges rather than replaces (HierarchyManager.restructureUnit), so
          // the prior refresh's children need clearing or they'd accumulate instead of being
          // replaced. Add-then-remove (not remove-then-add): if the add call fails, the QSet still
          // has its previous (stale but present) children, never zero. If the remove call fails
          // after a successful add, the QSet is briefly over-populated (old + new) until the next
          // refresh cleans it up — a much softer failure than an empty QuestionSet.
          val existingChildren = dynamicAssessHelper.parseExistingChildren(nodeProps)
          val added = dynamicAssessHelper.addQuestionsToSet(objectId, allSelectedIds)
          if (!added) {
            false
          } else {
            val staleChildren = existingChildren.diff(allSelectedIds)
            if (staleChildren.nonEmpty && !dynamicAssessHelper.removeQuestionsFromSet(objectId, staleChildren)) {
              logger.error(s"DynamicAssessFunction :: new selection added for $objectId but failed to remove stale children $staleChildren, QuestionSet is temporarily over-populated until next refresh")
            }
            true
          }
        case "Content" =>
          val items = dynamicAssessHelper.getQuestionsByIdentifiers(allSelectedIds)
          if (items.size < allSelectedIds.size) {
            // getQuestionsByIdentifiers drops failed fetches silently per-id; surface the gap here
            // so a partial ECML body doesn't get reported as a full, unqualified success.
            logger.warn(s"DynamicAssessFunction :: only fetched ${items.size}/${allSelectedIds.size} selected questions for $objectId, body will contain fewer questions than selected")
          }
          val ecmlBody = ECMLBodyBuilder.buildEcmlBodyFromItems(items, name, config)
          dynamicAssessHelper.updateContentBody(cassandraUtil, objectId, ecmlBody)
      }

      if (written) {
        metrics.incCounter(config.dynamicAssessSuccessCount)
        logger.info(s"DynamicAssessFunction :: wrote ${allSelectedIds.size} questions for $objectId")
        if (event.objectType == "Content") {
          // updateContentBody is a raw Cassandra write with no graph/ECAR change (see ECMLBodyBuilder doc comment);
          // loop a normal publish event back through PublishEventRouter so ECAR + versioning + offline packages
          // actually pick up the refreshed body, instead of leaving it Cassandra-only.
          val mimeType = Option(nodeProps.get("mimeType")).map(_.toString).getOrElse("")
          val pkgVersion = Option(nodeProps.get("pkgVersion")).map(_.toString.toDouble).getOrElse(0d)
          context.output(config.dynamicAssessRepublishOutTag, dynamicAssessHelper.buildRepublishEvent(objectId, mimeType, channel, pkgVersion))
        }
      } else {
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-write-back", "error" -> "write-back call failed")))
      }
    } catch {
      case e: Exception =>
        logger.error(s"DynamicAssessFunction :: failed for $objectId - ${e.getMessage}", e)
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-refresh", "error" -> e.getMessage)))
    }
  }
}
