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
      val nodeProps = if (objectId == rawObjectId) rawNodeProps
        else Option(janusGraphUtil.getNodeProperties(objectId)).getOrElse(rawNodeProps)

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

      // Step 1: availability counts per skill x difficulty, used for allocation only.
      val availability: (String, String) => Int = (skill, difficulty) =>
        dynamicAssessHelper.getAvailabilityCount(skill, difficulty, channel, categoryField, poolObjectType)

      // Step 2: minimum-first, availability-aware allocation, determines the required count per bucket.
      val allocationResult = dynamicAssessHelper.allocate(skills, difficultyTarget, minCriteria, availability)
      val allocations = allocationResult.allocations

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

      val written = baseObjectType match {
        case "QuestionSet" =>
          // baseObjectId, not objectId: /add and /remove self-resolve the editable copy, so a literal ".img" id risks double-resolution. Add-then-remove: /add merges not replaces, so this order never leaves the QSet empty on partial failure.
          val existingChildren = dynamicAssessHelper.parseExistingChildren(nodeProps)
          val added = dynamicAssessHelper.addQuestionsToSet(baseObjectId, allSelectedIds)
          if (!added) {
            false
          } else {
            val staleChildren = existingChildren.diff(allSelectedIds)
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
          val ecmlBody = ECMLBodyBuilder.buildEcmlBodyFromItems(items, name, config)
          dynamicAssessHelper.updateContentBody(cassandraUtil, objectId, ecmlBody)
      }

      if (written) {
        metrics.incCounter(config.dynamicAssessSuccessCount)
        logger.info(s"DynamicAssessFunction :: wrote ${allSelectedIds.size} questions for $objectId")
        if (baseObjectType == "Content") {
          // Body write is Cassandra-only; loop a publish event back with the base id (the pipeline resolves its own editable copy) so ECAR/versioning/offline actually pick it up.
          val mimeType = Option(nodeProps.get("mimeType")).map(_.toString).getOrElse("")
          context.output(config.dynamicAssessRepublishOutTag, dynamicAssessHelper.buildRepublishEvent(baseObjectId, mimeType, channel, pkgVersion))
        }
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
