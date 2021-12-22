package org.sunbird.spec

import org.neo4j.driver.v1.exceptions.ClientException
import org.sunbird.job.util.KafkaClientUtil

class KafkaClientSpec extends KafkaBaseTest {

//  "validate with valid topic name" should "return true" in {
//    createTopic("test.topic1")
//    val client = new KafkaClientUtil
//    val result = client.validate("test.topic1")
//    assert(result)
//  }

  "validate with invalid topic name" should "return false" in {
    val client = new KafkaClientUtil
    val result = client.validate("test.topic4")
    assert(!result)
  }

//  "send with valid topic name" should "send the message successfully to the topic" in {
//    val event = "{\"eid\":\"BE_JOB_REQUEST\",\"ets\":1546931576000,\"mid\":\"LP.1546931576000.b3fb188d-d6fe-431e-b528-da3780c710a8\",\"actor\":{\"id\":\"learning-service\",\"type\":\"System\"},\"context\":{\"pdata\":{\"ver\":\"1.0\",\"id\":\"org.ekstep.platform\"},\"channel\":\"in.ekstep\",\"env\":\"dev\"},\"object\":{\"ver\":1.0,\"id\":\"do_1234\"},\"edata\":{\"action\":\"link_dialcode\",\"iteration\":1,\"graphId\":\"domain\",\"contentType\":\"Course\",\"objectType\":\"Content\"}}"
//    val topic = "test.topic3"
//    createTopic(topic)
//    val client = new KafkaClientUtil
//    client.send(event, topic)
//    consumeFirstStringMessageFrom(topic) shouldBe event
//  }

  "send with invalid topic name" should "throw client exception" in {
    val event = "{\"eid\":\"BE_JOB_REQUEST\",\"ets\":1546931576000,\"mid\":\"LP.1546931576000.b3fb188d-d6fe-431e-b528-da3780c710a8\",\"actor\":{\"id\":\"learning-service\",\"type\":\"System\"},\"context\":{\"pdata\":{\"ver\":\"1.0\",\"id\":\"org.ekstep.platform\"},\"channel\":\"in.ekstep\",\"env\":\"dev\"},\"object\":{\"ver\":1.0,\"id\":\"do_1234\"},\"edata\":{\"action\":\"link_dialcode\",\"iteration\":1,\"graphId\":\"domain\",\"contentType\":\"Course\",\"objectType\":\"Content\"}}"
    val topic = "test.topic4"
    val client = new KafkaClientUtil
    val exception = intercept[ClientException] {
      client.send(event, topic)
    }
    exception.getMessage shouldEqual "Topic with name: " + topic + ", does not exists."
  }

}

