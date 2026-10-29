package org.sunbird.job.knowlg.function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.knowlg.publish.domain.Event
import org.sunbird.job.knowlg.publish.helpers.{DynamicAssessHelper, ECMLBodyBuilder}
import org.sunbird.job.knowlg.publish.helpers.DynamicAssessHelper.Allocation
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.util.{HttpUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

/** Dynamic Assess selection (docs/dynamic-assess-questionset-schema.md), triggered by edata.action = "refresh-body"; reads and writes only through knowlg-service, which owns the edit copy. */
class DynamicAssessFunction(config: KnowlgPublishConfig, httpUtil: HttpUtil,
                            @transient var dynamicAssessHelper: DynamicAssessHelper = null)
                           (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DynamicAssessFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    dynamicAssessHelper = new DynamicAssessHelper(config, httpUtil)
  }

  override def metricsList(): List[String] =
    List(config.dynamicAssessEventCount, config.dynamicAssessSuccessCount, config.dynamicAssessFailedCount, config.dynamicAssessSkippedCount)

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    val rawObjectId = event.identifier
    // "ContentImage"/"QuestionSetImage" are the editable-copy objectTypes for an already-published node; treat them the same as their base type.
    val baseObjectType = event.objectType.stripSuffix("Image")
    metrics.incCounter(config.dynamicAssessEventCount)
    logger.info(s"DynamicAssessFunction :: processing $rawObjectId, objectType=${event.objectType}")

    if (baseObjectType != "QuestionSet" && baseObjectType != "Content") {
      logger.info(s"DynamicAssessFunction :: unsupported objectType=${event.objectType}, skipping $rawObjectId")
      metrics.incCounter(config.dynamicAssessSkippedCount)
      return
    }

    try {
      // knowlg-service resolves the edit copy (".img") itself on every read and write below, so the job only ever uses the base id.
      val objectId = rawObjectId.stripSuffix(".img")
      // Settings as the editor saved them, from the edit copy when there is one; every write below goes through knowlg-service too, so without this read nothing could be written.
      val currentObject = dynamicAssessHelper.readCurrentObject(baseObjectType, objectId).getOrElse {
        logger.error(s"DynamicAssessFunction :: could not read $objectId from knowlg-service, skipping")
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-read",
          "error" -> "could not read the content's current settings (mode=edit) from knowlg-service")))
        return
      }
      // The selected skills live in the framework's deepest category field (e.g. "topic" for BMGS, "skill" for USF), so resolve it from the freshest framework before reading them.
      val framework = Option(currentObject.getOrElse("framework", null)).map(_.toString).getOrElse("")
      val categoryField = dynamicAssessHelper.resolveCategoryCode(framework)
      val nodeProps = dynamicAssessHelper.withCurrentSettings(new java.util.HashMap[String, AnyRef](), currentObject, List(categoryField))

      val skills = dynamicAssessHelper.parseSkills(nodeProps, categoryField)
      val difficultyTarget = dynamicAssessHelper.parseDifficultyTarget(nodeProps)
      val channel = Option(nodeProps.get("channel")).map(_.toString).getOrElse("")
      val name = Option(nodeProps.get("name")).map(_.toString).getOrElse(objectId)
      // The content's own values, so this job enforces what the editors and knowlg-service showed the author; config only covers content created before they were stored.
      val minCriteria = dynamicAssessHelper.nodeSetting(nodeProps, "minCriteria", 0, config.dynamicAssessMinCriteria)
      val multiplier = dynamicAssessHelper.nodeSetting(nodeProps, "multiplier", 1, config.dynamicAssessMultiplier)

      if (!dynamicAssessHelper.isFeasible(skills, difficultyTarget, minCriteria)) {
        logger.info(s"DynamicAssessFunction :: feasibility check failed for $objectId, skills=$skills, difficultyTarget=$difficultyTarget, minCriteria=$minCriteria")
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-feasibility",
          "error" -> "minCriteria x skills.length exceeds the shared E/M/D total")))
        return
      }

      // ECML and QuestionSet draw from separate pools: AssessmentItem vs Question.
      val poolObjectType = if (baseObjectType == "Content") "AssessmentItem" else "Question"

      // Step 1: availability per skill x difficulty; a bucket can only supply available / multiplier, so every selection has multiplier x its size to pick from.
      val availability: (String, String) => Int = (skill, difficulty) =>
        dynamicAssessHelper.effectiveAvailability(dynamicAssessHelper.getAvailabilityCount(skill, difficulty, channel, categoryField, poolObjectType), multiplier)

      // Step 2: minimum-first, availability-aware allocation, determines the required count per bucket.
      val allocationResult = dynamicAssessHelper.allocate(skills, difficultyTarget, minCriteria, availability)
      val allocations = allocationResult.allocations

      // A target the pool can't fully cover at the multiplier aborts the whole refresh, so a partial assessment is never written.
      val uncovered = dynamicAssessHelper.uncoveredLevels(allocations, difficultyTarget)
      if (uncovered.nonEmpty) {
        logger.info(s"DynamicAssessFunction :: insufficient pool for $objectId at multiplier=$multiplier, uncovered=$uncovered")
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-insufficient-pool", "multiplier" -> multiplier,
          "uncovered" -> uncovered.map { case (level, (covered, target)) => level -> Map("target" -> target, "coverable" -> covered) },
          "error" -> s"each difficulty needs at least target x $multiplier Live questions across the selected skills")))
        return
      }

      allocationResult.minCriteriaShortfalls.foreach { skill =>
        logger.info(s"DynamicAssessFunction :: $skill couldn't be reserved its own minCriteria=$minCriteria from its own pool for $objectId")
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-min-criteria",
          "skill" -> skill, "error" -> "skill's own pool couldn't satisfy minCriteria across any difficulty bucket")))
      }

      // Step 3: fetch required x multiplier candidates and select per allocation, sequentially excluding ids already claimed by an earlier bucket (a candidate can match more than one requested skill).
      val results = allocations.foldLeft(List.empty[DynamicAssessHelper.SkillDifficultyResult] -> Set.empty[String]) { case ((acc, usedIds), allocation) =>
        val result = dynamicAssessHelper.selectForAllocation(allocation, channel, categoryField, poolObjectType, multiplier, usedIds)
        (acc :+ result, usedIds ++ result.selectedIds)
      }._1

      val (fulfilled, shortfallResults): (List[(DynamicAssessHelper.SkillDifficultyResult, Allocation)], List[(DynamicAssessHelper.SkillDifficultyResult, Allocation)]) =
        results.zip(allocations).partition { case (result, allocation) => result.selectedIds.size >= dynamicAssessHelper.learnerPoolSize(allocation.requiredCount, multiplier) }

      shortfallResults.foreach { case (result, allocation) =>
        logger.info(s"DynamicAssessFunction :: shortfall for $objectId, skill=${allocation.skill}, difficulty=${allocation.difficulty}, required=${allocation.requiredCount}, got=${result.selectedIds.size}")
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-selection",
          "skill" -> allocation.skill, "difficulty" -> allocation.difficulty,
          "required" -> allocation.requiredCount, "available" -> result.selectedIds.size)))
      }

      // Any short bucket (e.g. candidates already claimed by another skill) aborts the whole refresh, same as an uncovered target.
      if (shortfallResults.nonEmpty) {
        metrics.incCounter(config.dynamicAssessFailedCount)
        logger.info(s"DynamicAssessFunction :: ${shortfallResults.size} bucket(s) short for $objectId, nothing written")
        return
      }

      val allSelectedIds = fulfilled.map(_._1).flatMap(_.selectedIds).distinct

      if (allSelectedIds.isEmpty) {
        metrics.incCounter(config.dynamicAssessFailedCount)
        logger.info(s"DynamicAssessFunction :: no questions selected for $objectId, all buckets shortfell")
        return
      }

      val written = baseObjectType match {
        case "QuestionSet" =>
          // Add-then-remove: /add merges not replaces, so this order never leaves the QSet empty on partial failure.
          val existingChildren = dynamicAssessHelper.childIds(currentObject)
          val added = dynamicAssessHelper.addQuestionsToSet(objectId, allSelectedIds)
          if (!added) {
            false
          } else {
            // Re-read after the add: the start-of-job childNodes misses anything attached since (e.g. by an editor save), which would otherwise stay in the pool.
            val currentChildren = dynamicAssessHelper.readCurrentChildren(objectId).getOrElse(existingChildren)
            val staleChildren = (currentChildren ++ existingChildren).distinct.diff(allSelectedIds)
            if (staleChildren.nonEmpty && !dynamicAssessHelper.removeQuestionsFromSet(objectId, staleChildren)) {
              logger.error(s"DynamicAssessFunction :: new selection added for $objectId but failed to remove stale children $staleChildren, QuestionSet is temporarily over-populated until next refresh")
            }
            true
          }
        case "Content" =>
          val items = dynamicAssessHelper.getAssessmentItemsByIdentifiers(allSelectedIds)
          if (items.size < allSelectedIds.size) {
            // Fetch drops failures silently per-id; warn so a partial body isn't reported as full success.
            logger.warn(s"DynamicAssessFunction :: only fetched ${items.size}/${allSelectedIds.size} selected questions for $objectId, body will contain fewer questions than selected")
          }
          // An item with no content, or a question-unit plugin the builder can't package, would render as a broken question.
          val (renderable, unrenderable) = items.partition(ECMLBodyBuilder.isRenderable)
          if (unrenderable.nonEmpty)
            logger.warn(s"DynamicAssessFunction :: skipping ${unrenderable.size} item(s) with no renderable question content for $objectId: ${unrenderable.flatMap(_.get("identifier")).mkString(",")}")
          if (renderable.isEmpty) {
            logger.error(s"DynamicAssessFunction :: no selected item has question content for $objectId, not writing an empty body")
            metrics.incCounter(config.dynamicAssessFailedCount)
            context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
              "objectId" -> objectId, "stage" -> "dynamic-assess-content",
              "error" -> "none of the selected AssessmentItems has renderable question content")))
            return
          }
          val ecmlBody = ECMLBodyBuilder.buildEcmlBodyFromItems(renderable, name, config, difficultyTarget, skills)
          // totalQuestions is what one learner sees, not the published pool size.
          val learnerCount = dynamicAssessHelper.learnerQuestionCount(difficultyTarget, renderable.map(i => Option(i.getOrElse("qlevel", null)).map(_.toString).getOrElse("")))
          val versionKey = Option(currentObject.getOrElse("versionKey", null)).map(_.toString).getOrElse("")
          dynamicAssessHelper.updateGeneratedContent(objectId, ecmlBody, learnerCount, versionKey)
      }

      if (written) {
        metrics.incCounter(config.dynamicAssessSuccessCount)
        // No republish, same as QuestionSet: generated questions go live only through review → publish.
        logger.info(s"DynamicAssessFunction :: wrote ${allSelectedIds.size} questions for $objectId")
      } else {
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> objectId, "stage" -> "dynamic-assess-write-back", "error" -> "write-back call failed")))
      }
    } catch {
      case e: Exception =>
        logger.error(s"DynamicAssessFunction :: failed for $rawObjectId - ${e.getMessage}", e)
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> rawObjectId, "stage" -> "dynamic-assess-refresh", "error" -> e.getMessage)))
    }
  }
}
