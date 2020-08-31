package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.sunbird.job.functions.PostPublishEventRouter
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.spec.BaseTestSpec

class PostPublishProcessorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: PostPublishProcessorConfig = new PostPublishProcessorConfig(config)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  "Post Publish Processor" should "read search copied contents" in {
    val identifier = "do_11300581751853056018"
    val list = new PostPublishEventRouter(jobConfig).getShallowCopiedContents(identifier)
    println(list)
  }

}
