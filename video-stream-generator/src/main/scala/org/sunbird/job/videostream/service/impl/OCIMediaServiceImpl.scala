package org.sunbird.job.videostream.service.impl

import org.slf4j.LoggerFactory
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.videostream.helpers.{JsonUtility, MediaRequest, MediaResponse, MediaServiceHelper, OCIRequestBody, Response, ResponseCode}
import org.sunbird.job.videostream.service.IMediaService
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig

import scala.collection.immutable.HashMap

object OCIMediaServiceImpl extends IMediaService {

  private[this] val logger = LoggerFactory.getLogger("OCIMediaServiceImpl")

  override def submitJob(request: MediaRequest)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {

    val inputUrl = request.request.getOrElse("artifactUrl", "").toString
    logger.info("inputUrl...{}",inputUrl)
    val contentId = request.request.get("identifier").mkString
    val compartment_id = config.getConfig("oci.compartment_id")
    logger.info("compartment_id...{}",compartment_id)
    val src_bucket = config.getConfig("oci.bucket.content_bucket_name")
    val dst_bucket = config.getConfig("oci.bucket.processed_bucket_name")
    val namespace = config.getConfig("oci.namespace")
    val temp = inputUrl.splitAt(inputUrl.lastIndexOf("/") + 1)
    val src_video = temp._2
    logger.info("src_video...{}",src_video)
    val prefix_input = config.getConfig("oci.stream.prefix_input")
    val media_flow_id = config.getConfig("oci.stream.work_flow_id")
    logger.info("media_flow_id...{}",media_flow_id)
    val mediaServiceHelper = new MediaServiceHelper()
    val mediaflowjobParameters = "{ \"video\": { \"srcBucket\": \"" + src_bucket + "\", \"dstBucket\": \""+ dst_bucket + "\", \"namespace\": \"" + namespace + "\", \"compartmentID\": \"" + compartment_id + "\", \"srcVideo\": \"" + src_video + "\", \"outputPrefixName\" : \"" + prefix_input + "\" } }"
    val mediaFlowResp = mediaServiceHelper.submitJob(compartment_id, media_flow_id, mediaServiceHelper.createJSONObject(mediaflowjobParameters))
    logger.info("mediaFlowResp.getId...{}",mediaFlowResp.getId)
    if (mediaFlowResp.getLifecycleState != "FAILED") {
      MediaResponse(mediaFlowResp.getId, System.currentTimeMillis().toString, new HashMap[String, AnyRef],
        ResponseCode.OK.toString, HashMap[String, AnyRef](
          "job" -> HashMap[String, AnyRef](
            "id" -> mediaFlowResp.getId,
            "status" -> mediaFlowResp.getLifecycleState.toString.toUpperCase,
            "submittedOn" -> mediaFlowResp.getTimeCreated.toString,
            "lastModifiedOn" -> mediaFlowResp.getTimeUpdated.toString
          )
        ))
    }else {
      Response.getFailureResponse(JsonUtility.toMap(mediaFlowResp.toString), "SERVER_ERROR", "Output Asset [ " + contentId + " ] Creation Failed for Job : " + mediaFlowResp.getId)
    }
  }

  override def getJob(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val mediaServiceHelper = new MediaServiceHelper()
    val mediaFlowResp = mediaServiceHelper.getWorkflowJob(jobId);
    if (mediaFlowResp.getLifecycleState != "FAILED") {
      MediaResponse(mediaFlowResp.getId, System.currentTimeMillis().toString, new HashMap[String, AnyRef],
        ResponseCode.OK.toString, JsonUtility.toMap(mediaFlowResp.toString))
    }else {
      Response.getFailureResponse(JsonUtility.toMap(mediaFlowResp.toString), "SERVER_ERROR", "Get WorkFlowJob Failed for the Id : " + mediaFlowResp.getId)
    }
  }

  override def getStreamingPaths(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val mediaServiceHelper = new MediaServiceHelper()
    val mediaWorkFlowId = config.getConfig("oci.stream.mediaWorkFlowId")
    val gatewayDomain = config.getConfig("oci.stream.gateway_domain")
    val streamUrl = mediaServiceHelper.getStreamingPaths(mediaWorkFlowId, gatewayDomain)
    Response.getSuccessResponse(HashMap[String, AnyRef]("streamUrl" -> streamUrl))
  }

  override def listJobs(listJobsRequest: MediaRequest): MediaResponse = {
    null
  }

  override def cancelJob(cancelJobRequest: MediaRequest): MediaResponse = {
    null
  }

}
