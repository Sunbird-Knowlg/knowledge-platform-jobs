package org.sunbird.job.util

import kong.unirest.Unirest

case class HTTPResponse(status: Int, body: String)

class HttpUtil {

  def get(url: String): HTTPResponse = {
    val response = Unirest.get(url).header("Content-Type", "application/json").asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

  def post(url: String, requestBody: String): HTTPResponse = {
    val response = Unirest.post(url).header("Content-Type", "application/json").body(requestBody).asString()
    HTTPResponse(response.getStatus, response.getBody)
  }

}
