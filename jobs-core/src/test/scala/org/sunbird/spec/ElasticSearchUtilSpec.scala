package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.elasticsearch.action.index.IndexResponse
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.util.ElasticSearchUtil


class ElasticSearchUtilSpec extends FlatSpec with Matchers {

  val config: Config = ConfigFactory.load("base-test.conf")
  val esUtil = new ElasticSearchUtil(config.getString("es.basePath"), "compositesearch", "cs")

//  "isIndexExists" should "return true if index exists" in {
//    val indexExists = esUtil.isIndexExists("compositesearch")
//    println("ElasticSearchUtilSpec:: isIndexExists test:: indexExists:: " + indexExists)
//
//    if(!indexExists)
//    {
//        val settings = """{"max_ngram_diff":"29","mapping":{"total_fields":{"limit":"1500"}},"analysis":{"filter":{"mynGram":{"token_chars":["letter","digit","whitespace","punctuation","symbol"],"min_gram":"1","type":"nGram","max_gram":"30"}},"analyzer":{"cs_index_analyzer":{"filter":["lowercase","mynGram"],"type":"custom","tokenizer":"standard"},"keylower":{"filter":"lowercase","tokenizer":"keyword"},"cs_search_analyzer":{"filter":["standard","lowercase"],"type":"custom","tokenizer":"standard"}}}}"""
//        val mappings = """{"dynamic_templates":[{"nested":{"match_mapping_type":"object","mapping":{"type":"nested","fields":{"type":"nested"}}}},{"longs":{"match_mapping_type":"long","mapping":{"type":"long","fields":{"raw":{"type":"long"}}}}},{"booleans":{"match_mapping_type":"boolean","mapping":{"type":"boolean","fields":{"raw":{"type":"boolean"}}}}},{"doubles":{"match_mapping_type":"double","mapping":{"type":"double","fields":{"raw":{"type":"double"}}}}},{"dates":{"match_mapping_type":"date","mapping":{"type":"date","fields":{"raw":{"type":"date"}}}}},{"strings":{"match_mapping_type":"string","mapping":{"type":"text","copy_to":"all_fields","analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer","fields":{"raw":{"type":"text","fielddata":true,"analyzer":"keylower"}}}}}],"properties":{"screenshots":{"type":"text","index":false},"body":{"type":"text","index":false},"appIcon":{"type":"text","index":false},"all_fields":{"type":"text","analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer","fields":{"raw":{"type":"text","fielddata":true,"analyzer":"keylower"}}}}}"""
//        esUtil.addIndex(settings, mappings)
//    }
//    assert(esUtil.isIndexExists("compositesearch"))
//  }
//
//
//  "addDocument" should "add document and return index" in {
//    val do_113405023736512512114Json = """{"identifier":"do_113405023736512512114","graph_id":"domain","node_id":0,"collections":["do_11340502373639782416", "do_113405023736479744112"],"objectType":"Collection","nodeType":"DATA_NODE"}"""
//    val documentIndexResponse: IndexResponse = esUtil.addDocument("do_113405023736512512114",do_113405023736512512114Json)
//    println("ElasticSearchUtilSpec:: addDocument test:: documentIndexResponse:: " + documentIndexResponse)
//  }
//
//  "getDocumentAsString" should "return Document Json" in {
//    val documentJson: String = esUtil.getDocumentAsString("do_113405023736512512114")
//    println("ElasticSearchUtilSpec:: getDocumentAsString test:: documentJson:: " + documentJson)
//  }

}
