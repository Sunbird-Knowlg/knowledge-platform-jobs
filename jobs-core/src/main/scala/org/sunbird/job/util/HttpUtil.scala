package org.sunbird.job.util

import kong.unirest.Unirest
import java.util

import scala.collection.JavaConverters._


case class HTTPResponse(status: Int, body: String) extends Serializable

class HttpUtil extends Serializable {

  def get(url: String): HTTPResponse = {
    val response = Unirest.get(url).header("Content-Type", "application/json").asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

  def post(url: String, requestBody: String, headers: util.Map[String, String] = Map("Content-Type" -> "application/json").asJava): HTTPResponse = {
    val response = Unirest.post(url).headers(headers).body(requestBody).asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

}
