package org.sunbird.job.videostream.helpers

object AzureRequestBody {

  val create_asset = " {\"properties\": {\"storageAccountName\": \"assetStorageAccountName\",\"description\": \"assetDescription\",\"alternateId\" : \"assetId\"}}"
  val submit_job = "{\"properties\": {\"input\": {\"@odata.type\": \"#Microsoft.Media.JobInputHttp\",\"baseUri\": \"baseInputUrl\",\"files\": [\"inputVideoFile\"]},\"outputs\": [{\"@odata.type\": \"#Microsoft.Media.JobOutputAsset\",\"assetName\": \"assetId\"}]}}"
  val create_stream_locator="{\"properties\":{\"assetName\": \"assetId\",\"streamingPolicyName\": \"policyName\"}}"
}
