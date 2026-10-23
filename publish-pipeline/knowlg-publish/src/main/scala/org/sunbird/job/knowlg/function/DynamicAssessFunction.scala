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

/** Dynamic Assess selection (docs/dynamic-assess-questionset-schema.md), triggered by edata.action = "refresh-body". */
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
      val rawNodeProps = Option(janusGraphUtil.getNodeProperties(rawObjectId))
        .getOrElse(throw new RuntimeException(s"Node not found in JanusGraph: $rawObjectId"))

      // Once published, all reads/writes must target the editable ".img" copy, not the live one, or metadata/children come from the wrong snapshot; recomputed here so a raw Kafka-triggered event can't fall out of sync with the API layer's own mode=edit resolution.
      val pkgVersion = Option(rawNodeProps.get("pkgVersion")).map(_.toString.toDouble).getOrElse(0d)
      val objectId = if (pkgVersion > 0 && !rawObjectId.endsWith(".img")) s"$rawObjectId.img" else rawObjectId
      val baseObjectId = objectId.stripSuffix(".img")
      val editCopyProps = if (objectId == rawObjectId) None else Option(janusGraphUtil.getNodeProperties(objectId))
      val graphProps = editCopyProps.getOrElse(rawNodeProps)
      // Settings come from knowlg-service, which saw the editor's save: this job's graph read can return the state from seconds before it.
      val currentObject = dynamicAssessHelper.readCurrentObject(baseObjectType, baseObjectId)
      if (currentObject.isEmpty) logger.warn(s"DynamicAssessFunction :: could not read current settings for $baseObjectId, using graph properties")
      val nodeProps = currentObject.map(dynamicAssessHelper.withCurrentSettings(graphProps, _)).getOrElse(graphProps)
      val metadataNodeId = dynamicAssessHelper.generatedMetadataNodeId(objectId, rawObjectId, editCopyProps.isDefined)

      // Published ECML must be regenerated into its edit copy (the refresh-body API creates it); without one the body would be orphaned, so fail loudly instead.
      if (baseObjectType == "Content" && metadataNodeId.isEmpty) {
        logger.error(s"DynamicAssessFunction :: no edit copy $objectId for published content $rawObjectId, skipping")
        metrics.incCounter(config.dynamicAssessFailedCount)
        context.output(config.failedEventOutTag, ScalaJsonUtil.serialize(Map(
          "objectId" -> rawObjectId, "stage" -> "dynamic-assess-edit-copy",
          "error" -> "published content has no .img edit copy; regenerate via the refresh-body API, which creates it")))
        return
      }

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
      // ECML and QuestionSet draw from separate pools: AssessmentItem vs Question.
      val poolObjectType = if (baseObjectType == "Content") "AssessmentItem" else "Question"

      // Step 1: availability per skill x difficulty; a bucket can only supply available / multiplier, so every selection has multiplier x its size to pick from.
      val multiplier = config.dynamicAssessMultiplier
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
        val result = dynamicAssessHelper.selectForAllocation(allocation, channel, categoryField, poolObjectType, usedIds)
        (acc :+ result, usedIds ++ result.selectedIds)
      }._1

      val (fulfilled, shortfallResults): (List[(DynamicAssessHelper.SkillDifficultyResult, Allocation)], List[(DynamicAssessHelper.SkillDifficultyResult, Allocation)]) =
        results.zip(allocations).partition { case (result, allocation) => result.selectedIds.size >= dynamicAssessHelper.learnerPoolSize(allocation.requiredCount) }

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
          // baseObjectId, not objectId: /add and /remove self-resolve the editable copy, so a literal ".img" id risks double-resolution. Add-then-remove: /add merges not replaces, so this order never leaves the QSet empty on partial failure.
          val existingChildren = currentObject.map(dynamicAssessHelper.childIds).getOrElse(dynamicAssessHelper.parseExistingChildren(nodeProps))
          val added = dynamicAssessHelper.addQuestionsToSet(baseObjectId, allSelectedIds)
          if (!added) {
            false
          } else {
            // Re-read after the add: the start-of-job childNodes misses anything attached since (e.g. by an editor save), which would otherwise stay in the pool.
            val currentChildren = dynamicAssessHelper.readCurrentChildren(baseObjectId).getOrElse(existingChildren)
            val staleChildren = (currentChildren ++ existingChildren).distinct.diff(allSelectedIds)
            if (staleChildren.nonEmpty && !dynamicAssessHelper.removeQuestionsFromSet(baseObjectId, staleChildren)) {
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
          val bodyWritten = dynamicAssessHelper.updateContentBody(cassandraUtil, objectId, ecmlBody)
          // totalQuestions is what one learner sees, not the published pool size.
          val learnerCount = dynamicAssessHelper.learnerQuestionCount(difficultyTarget, renderable.map(i => Option(i.getOrElse("qlevel", null)).map(_.toString).getOrElse("")))
          if (bodyWritten) metadataNodeId.foreach(id => janusGraphUtil.updateNode(id, dynamicAssessHelper.generatedContentMetadata(learnerCount)))
          bodyWritten
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
