package org.sunbird.job.spec

import java.io.IOException

import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import org.sunbird.job.task.VideoStreamGeneratorConfig

class MockAzureWebServer(jobConfig: VideoStreamGeneratorConfig) {
  val server = new MockWebServer()

  val accessTokenResp = """{"token_type":"Bearer","expires_in":"3599","ext_expires_in":"3599","expires_on":"1605789466","not_before":"1605785566","resource":"https://management.core.windows.net/","access_token":"testToken"}"""

  val assetJson = """{"name":"asset-do_3126597193576939521910_1605816926271","id":"/subscriptions/aaaaaaaa-6899-4ef6-aaaa-5a185b3b7254/resourceGroups/sunbird-devnew-env/providers/Microsoft.Media/mediaservices/sunbirddevmedia/assets/asset-do_3126597193576939521910_1605816926271","type":"Microsoft.Media/mediaservices/assets","properties":{"assetId":"aaaaaaa-13bb-45c7-aaaa-32ac2e97cf12","created":"2020-11-19T20:16:54.463Z","lastModified":"2020-11-19T20:20:33.613Z","alternateId":"asset-do_3126597193576939521910_1605816926271","description":"Output Asset for do_3126597193576939521910_1605816926271","container":"asset-aaaaaaa-13bb-45c7-b186-32ac2e97cf12","storageAccountName":"sunbirddevmedia","storageEncryptionFormat":"None"}}"""

  val submitJobJson = """{"name":"do_3126597193576939521910_1605816926271","id":"/subscriptions/aaaaaaaa-6899-4ef6-8a14-5a185b3b7254/resourceGroups/sunbird-devnew-env/providers/Microsoft.Media/mediaservices/sunbirddevmedia/transforms/media_transform_default/jobs/do_3126597193576939521910_1605816926271","type":"Microsoft.Media/mediaservices/transforms/jobs","properties":{"created":"2020-11-19T20:26:49.7953248Z","state":"Scheduled","input":{"@odata.type":"#Microsoft.Media.JobInputHttp","files":["test.mp4"],"baseUri":"https://sunbirded.com/"},"lastModified":"2020-11-19T20:26:49.7953248Z","outputs":[{"@odata.type":"#Microsoft.Media.JobOutputAsset","state":"Queued","progress":0,"label":"BuiltInStandardEncoderPreset_0","assetName":"asset-do_3126597193576939521910_1605816926271"}],"priority":"Normal","correlationData":{}}}"""

  val getJobJson = """{"name":"do_3126597193576939521910_1605816926271","job":{"status":"Finished"}}"""

  val getStreamUrlJson = """{"streamUrl":"https://dev.sunbirded.org/testurl"}"""

  @throws[IOException]
  def submitRestUtilData: Unit = {
    try
      server.start(8099)
    catch {
      case e: IOException =>
        System.out.println("Exception" + e)
    }
    server.enqueue(new MockResponse().setBody(accessTokenResp))
//    server.url(jobConfig.getConfig("azure.login.endpoint")+"/"+jobConfig.getSystemConfig("azure.subscription.id")+"/oauth2/token")

    server.enqueue(new MockResponse().setBody(assetJson))
//    server.url(jobConfig.getConfig("azure.api.endpoint")+"/subcriptions/"+jobConfig.getSystemConfig("azure.subscription.id")+"/resourceGroups/"+jobConfig.getSystemConfig("azure.resource.group.name")+"/providers/Microsoft.Media/mediaServices/"+jobConfig.getSystemConfig("azure.account.name")+"/assets/asset-do_3126597193576939521910_1?api-version=2018-07-01")

    server.enqueue(new MockResponse().setBody(accessTokenResp))
//    server.url(jobConfig.getConfig("azure.login.endpoint")+"/"+jobConfig.getSystemConfig("azure.subscription.id")+"/oauth2/token")

    server.enqueue(new MockResponse().setBody(submitJobJson))
  }

  @throws[IOException]
  def processRestUtilData:Unit = {

    //    server.url(jobConfig.getConfig("azure.api.endpoint")+"/subcriptions/"+jobConfig.getSystemConfig("azure.subscription.id")+"/resourceGroups/"+jobConfig.getSystemConfig("azure.resource.group.name")+"/providers/Microsoft.Media/mediaServices/"+jobConfig.getSystemConfig("azure.account.name")+"/transforms/media_transform_default/jobs/do_3126597193576939521910_1?api-version=2018-07-01")

    server.enqueue(new MockResponse().setBody(accessTokenResp))

    server.enqueue(new MockResponse().setBody(assetJson))

    server.enqueue(new MockResponse().setBody(accessTokenResp))

    server.enqueue(new MockResponse().setBody(submitJobJson))
    //    server.url(jobConfig.getConfig("azure.login.endpoint")+"/"+jobConfig.getSystemConfig("azure.subscription.id")+"/oauth2/token")

    server.enqueue(new MockResponse().setBody(getJobJson))
    //    server.url(jobConfig.getConfig("azure.api.endpoint")+"/subcriptions/"+jobConfig.getSystemConfig("azure.subscription.id")+"/resourceGroups/"+jobConfig.getSystemConfig("azure.resource.group.name")+"/providers/Microsoft.Media/mediaServices/"+jobConfig.getSystemConfig("azure.account.name")+"/transforms/media_transform_default/jobs/do_3126597193576939521910_1?api-version=2018-07-01")

    server.enqueue(new MockResponse().setBody(getStreamUrlJson))

    server.enqueue(new MockResponse().setBody(getStreamUrlJson))

    server.enqueue(new MockResponse().setBody(getJobJson))

    server.enqueue(new MockResponse().setBody(getStreamUrlJson))

    server.enqueue(new MockResponse().setBody(getStreamUrlJson))

    server.enqueue(new MockResponse().setBody(getJobJson))

    server.enqueue(new MockResponse().setBody(getStreamUrlJson))

    server.enqueue(new MockResponse().setBody(getStreamUrlJson))


    server.enqueue(new MockResponse().setHeader("Authorization", "auth_token"))
  }

  def close(): Unit = {
    server.close()
  }
}
