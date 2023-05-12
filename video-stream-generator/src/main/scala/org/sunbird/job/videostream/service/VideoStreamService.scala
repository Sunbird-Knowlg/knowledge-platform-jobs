package org.sunbird.job.videostream.service

import java.util.UUID

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.videostream.service.impl.MediaServiceFactory
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.job.videostream.helpers.{JobRequest, MediaRequest, MediaResponse, StreamingStage}

import scala.collection.JavaConverters._

class VideoStreamService(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil) {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[VideoStreamService])
  private lazy val mediaService = MediaServiceFactory.getMediaService(config)
  private lazy val dbKeyspace:String = config.dbKeyspace
  private lazy val dbTable:String = config.dbTable
  lazy val cassandraUtil:CassandraUtil = new CassandraUtil(config.lmsDbHost, config.lmsDbPort, config)
  private lazy val clientKey:String = "SYSTEM_LP"
  private lazy val SUBMITTED:String = "SUBMITTED"
  private lazy val VIDEO_STREAMING:String = "VIDEO_STREAMING"

  def submitJobRequest(eData: Map[String, AnyRef]): Unit = {
    val stageName = "STREAMING_JOB_SUBMISSION";
    val jobSubmitted = DateTime.now()
    val requestId = UUID.randomUUID().toString
    val jobRequest = JobRequest(clientKey, requestId, None, SUBMITTED, JSONUtil.serialize(eData), 0, Option(jobSubmitted),
                                Option(eData.getOrElse("artifactUrl", "").asInstanceOf[String]), None, None, None, None, None,
                                None, None, None, None, None, None, None, Option(stageName), Option(SUBMITTED), Option(VIDEO_STREAMING))

    saveJobRequest(jobRequest)
    submitStreamJob(jobRequest)
  }

  def processJobRequest(metrics: Metrics): Unit = {
    updateProcessingRequest(metrics)
    resubmitFailedJob()
  }

  def updateProcessingRequest(metrics: Metrics): Unit = {
    val processingJobRequests = readFromDB(Map("status" -> "PROCESSING"))
    val stageName = "STREAMING_JOB_COMPLETE"

    for (jobRequest <- processingJobRequests) {
      val iteration = jobRequest.iteration
      val streamStage = if (jobRequest.job_id != None) {
        val mediaResponse:MediaResponse = mediaService.getJob(jobRequest.job_id.get)
        logger.info("Get job details while saving: " + JSONUtil.serialize(mediaResponse.result))
        logger.info("updateProcessingRequest mediaResponse.responseCode: " + mediaResponse.responseCode)
        if(mediaResponse.responseCode.contentEquals("OK")) {
          val job = mediaResponse.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]]
          val jobStatus = job.getOrElse("status","").asInstanceOf[String]
          val workFlowJobId = job.getOrElse("id","").asInstanceOf[String]
          logger.info("updateProcessingRequest job: " + job)
          logger.info("updateProcessingRequest workFlowJobId: " + workFlowJobId)
          if(config.jobStatus.contains(jobStatus)) {
            val streamingUrl = mediaService.getStreamingPaths(workFlowJobId).result.getOrElse("streamUrl","").asInstanceOf[String]
            val requestData = JSONUtil.deserialize[Map[String, AnyRef]](jobRequest.request_data)
            val contentId = requestData.getOrElse("identifier", "").asInstanceOf[String]
            val channel = requestData.getOrElse("channel", "").asInstanceOf[String]
            logger.info("Before calling updatePreviewUrl : contentId ::" + contentId+" streamingUrl ::"+streamingUrl+" channel ::"+channel)
            if(updatePreviewUrl(contentId, streamingUrl, channel)) {
              logger.info("updatePreviewUrl COMPLETE : contentId ::" + contentId)
              StreamingStage(jobRequest.request_id, jobRequest.client_key, jobRequest.job_id.get, stageName, jobStatus, "FINISHED", iteration + 1);
            } else {
              // Set job status to FAILED
              val errMessage = job.getOrElse("error", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("errorMessage", "Transcoding failed").asInstanceOf[String]
              StreamingStage(jobRequest.request_id, jobRequest.client_key, jobRequest.job_id.get, stageName, jobStatus, "FAILED", iteration + 1, errMessage)
            }
          } else if(jobStatus.equalsIgnoreCase("ERROR")){
            val errMessage = job.getOrElse("error", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("errorMessage", "No error message").asInstanceOf[String]
            StreamingStage(jobRequest.request_id, jobRequest.client_key, jobRequest.job_id.get, stageName, jobStatus, "FAILED", iteration + 1, errMessage)
          } else {
            val errMessage = job.getOrElse("error", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("errorMessage", "No error message").asInstanceOf[String]
            StreamingStage(jobRequest.request_id, jobRequest.client_key, jobRequest.job_id.get, stageName, jobStatus, "FAILED", iteration + 1, errMessage)
          }
        } else {
          val errorMsg = mediaResponse.result.toString
          StreamingStage(jobRequest.request_id, jobRequest.client_key, null, stageName, "FAILED", "FAILED", iteration + 1, errorMsg);
        }
      } else {
        StreamingStage(jobRequest.request_id, jobRequest.client_key, null, stageName, "FAILED", "FAILED", iteration + 1, jobRequest.err_message.getOrElse(""));
      }

      if (streamStage != null) {
        val counter = if (streamStage.status.equals("FINISHED")) config.successEventCount else {
          if (streamStage.iteration <= config.maxRetries) config.retryEventCount else config.failedEventCount
        }
        metrics.incCounter(counter)
        updateJobRequestStage(streamStage)
      }
    }
  }

  def resubmitFailedJob(): Unit = {
    val failedJobRequests = readFromDB(Map("status" -> "FAILED", "iteration" -> Map("type"-> "lte", "value" -> config.maxRetries))).toArray
    failedJobRequests.foreach { jobRequest =>
      submitStreamJob(jobRequest)
    }
  }

  def submitStreamJob(jobRequest: JobRequest): Unit = {

    val requestData = JSONUtil.deserialize[Map[String, AnyRef]](jobRequest.request_data)
    val mediaRequest = MediaRequest(UUID.randomUUID().toString, null, requestData)
    val response:MediaResponse = mediaService.submitJob(mediaRequest)
    val stageName = "STREAMING_JOB_SUBMISSION"
    var streamStage:Option[StreamingStage] = None

    if (response.responseCode.equals("OK")) {
      val jobId = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("id","").asInstanceOf[String];
      val jobStatus = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("status","").asInstanceOf[String];
      streamStage = Option(StreamingStage(jobRequest.request_id, jobRequest.client_key, jobId, stageName, jobStatus, "PROCESSING", jobRequest.iteration + 1))
    } else {
      val errorMsg = response.result.toString

      streamStage = Option(StreamingStage(jobRequest.request_id, jobRequest.client_key, null, stageName, "FAILED", "FAILED", jobRequest.iteration + 1, errorMsg));
    }

    updateJobRequestStage(streamStage.get);
  }

  private def updatePreviewUrl(contentId: String, streamingUrl: String, channel: String): Boolean = {
    if(streamingUrl.nonEmpty && contentId.nonEmpty) {
      val requestBody = "{\"request\": {\"content\": {\"streamingUrl\":\""+ streamingUrl +"\"}}}"
      val url = config.lpURL + config.contentV4Update + contentId
      val headers = Map[String, String]("X-Channel-Id" -> channel, "Content-Type"->"application/json")
      logger.info("Before updating streamingUrl : url ::" + url+" requestBody ::"+requestBody)
      val response:HTTPResponse = httpUtil.patch(url, requestBody, headers)
      logger.info("updatePreviewUrl() response.status ::" + response.status)
      if(response.status == 200){
        logger.info("StreamingUrl updated successfully. url ::" + url+" contentId ::"+contentId)
        true
      } else {
        logger.error("Error while updating previewUrl for content : " + contentId + " :: "+response.body)
//        throw new Exception("Error while updating previewUrl for content : " + contentId + " :: "+response.body)
        false
      }
    } else {
      false
    }
  }

  def readFromDB(columns: Map[String, AnyRef]): List[JobRequest] = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(dbKeyspace, dbTable)
      .allowFiltering()
      .where()

    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case value: Map[String, AnyRef] =>
          if (value("type") == "lte") {
            selectWhere.and(QueryBuilder.lte(col._1, value("value")))
          } else {
            selectWhere.and(QueryBuilder.gte(col._1, value("value")))
          }
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })

    selectWhere.and(QueryBuilder.eq("job_name", VIDEO_STREAMING))
    logger.info("readFromDB : Query ::" + selectWhere.toString)
    val result = cassandraUtil.find(selectWhere.toString).asScala.toList.map { jr =>
      JobRequest(jr.getString("client_key"), jr.getString("request_id"), Option(jr.getString("job_id")), jr.getString("status"), jr.getString("request_data"), jr.getInt("iteration"), stage=Option(jr.getString("stage")), stage_status=Option(jr.getString("stage_status")),job_name=Option(jr.getString("job_name")))
    }
    result
  }

  def saveJobRequest(jobRequest: JobRequest): Boolean = {
    val query = QueryBuilder.insertInto(dbKeyspace, dbTable)
      .value("client_key", jobRequest.client_key)
      .value("request_id", jobRequest.request_id)
      .value("job_id", jobRequest.job_id.getOrElse(""))
      .value("status", jobRequest.status)
      .value("request_data", jobRequest.request_data)
      .value("iteration", jobRequest.iteration)
      .value("dt_job_submitted", setDateColumn(jobRequest.dt_job_submitted).get)
      .value("location", jobRequest.location.get)
      .value("stage", jobRequest.stage.get)
      .value("stage_status", jobRequest.stage_status.get)
      .value("job_name", jobRequest.job_name.get)

    val result = cassandraUtil.session.execute(query)
    result.wasApplied()
  }

  def updateJobRequestStage(streamStage: StreamingStage): Boolean = {
    val query = QueryBuilder.update(dbKeyspace, dbTable)
      .`with`(QueryBuilder.set("job_id", streamStage.job_id))
      .and(QueryBuilder.set("stage", streamStage.stage))
      .and(QueryBuilder.set("stage_status", streamStage.stage_status))
      .and(QueryBuilder.set("status", streamStage.status))
      .and(QueryBuilder.set("iteration", streamStage.iteration))
      .and(QueryBuilder.set("err_message", streamStage.err_message))
      .where(QueryBuilder.eq("request_id", streamStage.request_id))
      .and(QueryBuilder.eq("client_key", streamStage.client_key))

    cassandraUtil.upsert(query.toString)
  }

  def setDateColumn(date: Option[DateTime]): Option[Long] = {
    val timestamp = date.orNull
    if (null == timestamp) None else Option(timestamp.getMillis)
  }

  def closeConnection(): Unit = {
    cassandraUtil.close()
  }
}
