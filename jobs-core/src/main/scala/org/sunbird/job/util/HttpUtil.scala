package org.sunbird.job.util

import kong.unirest.Unirest
import org.apache.commons.collections.CollectionUtils

import scala.collection.JavaConverters._

case class HTTPResponse(status: Int, body: String) extends Serializable {
  def isSuccess:Boolean = Array(200, 201) contains status
}

class HttpUtil extends Serializable {

  def get(url: String, headers: Map[String, String] = Map[String, String]("Content-Type"->"application/json")): HTTPResponse = {
    val response = Unirest.get(url).headers(headers.asJava).asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

  def post(url: String, requestBody: String, headers: Map[String, String] = Map[String, String]("Content-Type"->"application/json")): HTTPResponse = {
    val response = Unirest.post(url).headers(headers.asJava).body(requestBody).asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

  def post_map(url: String, requestBody: Map[String, AnyRef], headers: Map[String, String] = Map[String, String]("Content-Type"->"application/json")): HTTPResponse = {
    val response = Unirest.post(url).headers(headers.asJava).fields(requestBody.asJava).asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

  def put(url: String, requestBody: String, headers: Map[String, String] = Map[String, String]("Content-Type"->"application/json")): HTTPResponse = {
    val response = Unirest.put(url).headers(headers.asJava).body(requestBody).asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

  def patch(url: String, requestBody: String, headers: Map[String, String] = Map[String, String]("Content-Type"->"application/json")): HTTPResponse = {
    val response = Unirest.patch(url).headers(headers.asJava).body(requestBody).asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

  def getSize(url: String, headers: Map[String, String] = Map[String, String]("Content-Type"->"application/json")):Int = {
    val resp = Unirest.head(url).headers(headers.asJava).asString()
    if (null != resp && resp.getStatus == 200) {
      val contentLength = if (CollectionUtils.isNotEmpty(resp.getHeaders.get("Content-Length"))) resp.getHeaders.get("Content-Length") else resp.getHeaders.get("content-length")
      if (CollectionUtils.isNotEmpty(contentLength)) contentLength.get(0).toInt else 0
    } else {
      val msg = s"Unable to get metadata for : $url | status : ${resp.getStatus}, body: ${resp.getBody}"
      throw new Exception(msg)
    }
  }
}

