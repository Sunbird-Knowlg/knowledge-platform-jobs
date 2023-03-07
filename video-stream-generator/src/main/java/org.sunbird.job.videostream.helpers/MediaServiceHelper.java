package org.sunbird.job.videostream.helpers;

import java.util.List;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.*;
import com.oracle.bmc.mediaservices.model.*;
import com.oracle.bmc.mediaservices.requests.*;
import com.oracle.bmc.mediaservices.responses.*;
import org.json.simple.JSONObject;

import com.oracle.bmc.mediaservices.MediaServicesClient;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MediaServiceHelper {

    public static Logger logger = LoggerFactory.getLogger(MediaServiceHelper.class);

    public JSONObject createJSONObject(String jsonString) {
        JSONObject jsonObject = new JSONObject();
        JSONParser jsonParser = new JSONParser();
        if ((jsonString != null) && !(jsonString.isEmpty())) {
            try {
                jsonObject = (JSONObject) jsonParser.parse(jsonString);
            } catch (org.json.simple.parser.ParseException e) {
                e.printStackTrace();
            }
        }
        return jsonObject;
    }

    public MediaWorkflowJob submitJob(String compartment_id, String mediaWorkFlowId,
                                      JSONObject mediaFlowJobParameters) {
        MediaServicesClient mediaClient = connectMediaService();
        logger.debug("mediaFlowJobParameters...." + mediaFlowJobParameters + "  mediaWorkFlowId..." + mediaWorkFlowId
                + " compartment_id..." + compartment_id);
        CreateMediaWorkflowJobRequest request = CreateMediaWorkflowJobRequest.builder()
                .createMediaWorkflowJobDetails(CreateMediaWorkflowJobByIdDetails.builder()
                        .displayName("media-flow-job-diksha").compartmentId(compartment_id)
                        .mediaWorkflowId(mediaWorkFlowId).parameters(mediaFlowJobParameters).build())
                .build();
        CreateMediaWorkflowJobResponse response = mediaClient.createMediaWorkflowJob(request);
        logger.debug("JOB_ID::{}", response.getMediaWorkflowJob().getId());
        closeMediaClient(mediaClient);
        return response.getMediaWorkflowJob();
    }

    public String getStreamingPaths(String mediaWorkflowId, String gatewayDomain) {
        MediaServiceHelper helper = new MediaServiceHelper();
        MediaServicesClient mediaClient = helper.connectMediaService();
        MediaWorkflowJob job = getWorkflowJob(mediaWorkflowId);
        List<JobOutput> outputList = job.getOutputs();
        String mediaAssetId = null;
        for (JobOutput output : outputList) {
            if (output.getObjectName().contains("master.m3u8")) {
                mediaAssetId = output.getId();
                break;
            }
        }
        String streamingURL = "https://" + gatewayDomain
                + "/" + mediaAssetId + "/master.m3u8";
        closeMediaClient(mediaClient);
        return streamingURL;
    }

    // Get Media Workflow Job
    public MediaWorkflowJob getWorkflowJob(String mediaWorkflowId) {
        logger.debug(" <<<< Entering getWorkflowJob() >>>> mediaWorkflowId::" + mediaWorkflowId);
        MediaServiceHelper helper = new MediaServiceHelper();
        MediaServicesClient mediaClient = helper.connectMediaService();
        GetMediaWorkflowJobRequest request = GetMediaWorkflowJobRequest.builder().mediaWorkflowJobId(mediaWorkflowId)
                .build();
        GetMediaWorkflowJobResponse response = mediaClient.getMediaWorkflowJob(request);
        MediaWorkflowJob mediaWorkflowJob = response.getMediaWorkflowJob();
        logger.debug("mediaWorkflowJob::::::::::::::" + mediaWorkflowJob);
        closeMediaClient(mediaClient);
        logger.debug(" <<<< Exiting getWorkflowJob() >>>> mediaWorkflowId::" + mediaWorkflowId);
        return mediaWorkflowJob;
    }

    public MediaServicesClient connectMediaService() {
        logger.debug("<<<< Entering connectMediaService() >>>>");
        MediaServicesClient mediaClient = null;
        try {
//            final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parse("~/.oci/config", "SUNBIRD");
//            AuthenticationDetailsProvider authenticationDetailsProvider = new ConfigFileAuthenticationDetailsProvider(configFile);

            InstancePrincipalsAuthenticationDetailsProvider authenticationDetailsProvider = InstancePrincipalsAuthenticationDetailsProvider
                    .builder().build();

            mediaClient = MediaServicesClient.builder().build(authenticationDetailsProvider);
            logger.debug("mediaClient::::::::::::::" + mediaClient);
        } catch (Exception myThrowableObject) {
            myThrowableObject.printStackTrace();
        }
        logger.debug("<<<< Exiting connectMediaService() >>>>");
        return mediaClient;
    }

    // Close Media Service Client
    private void closeMediaClient(MediaServicesClient mc) {
        mc.close();
    }
}

