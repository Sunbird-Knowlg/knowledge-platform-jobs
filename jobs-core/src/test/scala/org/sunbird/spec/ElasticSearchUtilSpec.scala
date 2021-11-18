package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.elasticsearch.action.index.IndexResponse
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.util.ElasticSearchUtil


class ElasticSearchUtilSpec extends FlatSpec with Matchers {

  val config: Config = ConfigFactory.load("base-test.conf")
  val esUtil = new ElasticSearchUtil(config.getString("es.basePath"), "compositesearch", "cs")

  "isIndexExists" should "return true if index exists" in {
    val indexExists = esUtil.isIndexExists("compositesearch")
    println("ElasticSearchUtilSpec:: isIndexExists test:: indexExists:: " + indexExists)
  }

  "addDocument" should "add document and return index" in {
    val do_113405023736512512114Json = """{"identifier":"do_113405023736512512114","graph_id":"domain","node_id":0,"collections":["do_11340502373639782416", "do_113405023736479744112"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
    val documentIndexResponse: IndexResponse = esUtil.addDocument("do_113405023736512512114",do_113405023736512512114Json)
    println("ElasticSearchUtilSpec:: addDocument test:: documentIndexResponse:: " + documentIndexResponse)
  }

  "getDocumentAsString" should "return Document Json" in {
    val documentJson: String = esUtil.getDocumentAsString("do_113405023736512512114")
    println("ElasticSearchUtilSpec:: getDocumentAsString test:: documentJson:: " + documentJson)
  }


}
