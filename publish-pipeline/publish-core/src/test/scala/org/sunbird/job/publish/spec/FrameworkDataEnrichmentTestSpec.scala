package org.sunbird.job.publish.spec

import java.util

import org.mockito.Mockito.when
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.cache.local.FrameworkMasterCategoryMap
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.publish.helpers.FrameworkDataEnrichment
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

import scala.collection.JavaConverters._

class FrameworkDataEnrichmentTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

	implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
	implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())

	override protected def beforeAll(): Unit = {
		super.beforeAll()
	}

	override protected def afterAll(): Unit = {
		super.afterAll()
	}

	"enrichFrameworkData" should "enrich the framework metadata" in {
		enrichFrameworkMasterCategoryMap()

		val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "IL_UNIQUE_ID" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "framework" -> "NCF", "targetFWIds" -> List("TPD", "NCFCOPY").asJava, "mediumIds" -> List("ncf_medium_telugu").asJava, "targetMediumIds" -> List("ncf_medium_english").asJava, "boardIds" -> List("ncf_board_cbse").asJava, "targetBoardIds" -> List("ncfcopy_board_ncert").asJava))
		when(mockNeo4JUtil.getNodesName(ArgumentMatchers.anyObject())).thenReturn(Map[String, String]("ncf_medium_telugu" -> "Telugu", "ncf_medium_english" -> "English",  "ncf_board_cbse" -> "CBSE", "ncfcopy_board_ncert" -> "NCERT"))
		val obj = new TestFrameworkDataEnrichment()
		val result = obj.enrichFrameworkData(data)
		result.metadata.getOrElse("se_mediumIds", List()).asInstanceOf[List[String]] should have length(2)
		result.metadata.getOrElse("se_mediumIds", List()).asInstanceOf[List[String]].contains("ncf_medium_telugu") should be (true)
		result.metadata.getOrElse("se_boardIds", List()).asInstanceOf[List[String]] should have length(2)
		result.metadata.getOrElse("se_boardIds", List()).asInstanceOf[List[String]].contains("ncf_board_cbse") should be (true)
		result.metadata.getOrElse("se_FWIds", List()).asInstanceOf[List[String]] should have length(3)
		result.metadata.getOrElse("se_FWIds", List()).asInstanceOf[List[String]].contains("TPD") should be (true)
		result.metadata.getOrElse("se_mediums", List()).asInstanceOf[List[String]] should have length(2)
		result.metadata.getOrElse("se_mediums", List()).asInstanceOf[List[String]].contains("English") should be (true)
		result.metadata.getOrElse("se_boards", List()).asInstanceOf[List[String]] should have length(2)
		result.metadata.getOrElse("se_boards", List()).asInstanceOf[List[String]].contains("CBSE") should be (true)
	}

	"enrichFrameworkData with only targetFramework" should "enrich only se_FWIds" in {
		val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "IL_UNIQUE_ID" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "framework" -> "NCF", "targetFWIds" -> List("TPD", "NCFCOPY").asJava))
		when(mockNeo4JUtil.getNodesName(ArgumentMatchers.anyObject())).thenReturn(Map[String, String]("ncf_medium_telugu" -> "Telugu", "ncf_medium_english" -> "English",  "ncf_board_cbse" -> "CBSE", "ncfcopy_board_ncert" -> "NCERT"))
		val obj = new TestFrameworkDataEnrichment()
		val result = obj.enrichFrameworkData(data)
		result.metadata.contains("se_mediumIds") should be (false)
		result.metadata.contains("se_boardIds") should be (false)
		result.metadata.contains("se_FWIds") should be (true)
		result.metadata.contains("se_mediums") should be (false)
		result.metadata.contains("se_boards") should be (false)
		result.metadata.getOrElse("se_FWIds", List()).asInstanceOf[List[String]] should have length(3)
		result.metadata.getOrElse("se_FWIds", List()).asInstanceOf[List[String]].contains("TPD") should be (true)
	}

	"getFrameworkCategoryMetadata from database" should "return touple values" in {
		when(mockNeo4JUtil.getNodePropertiesWithObjectType(ArgumentMatchers.anyString())).thenReturn(getNeo4jData())
		//enrichFrameworkMasterCategoryMap()
		val node : (List[String], Map[(String, String), List[String]]) = new TestFrameworkDataEnrichment().getFrameworkCategoryMetadata("domain", "Category")
		node._1.asInstanceOf[List[String]] should have length(6)
		node._2.asInstanceOf[Map[(String, String), List[String]]].size.equals(3) should be (true)
	}

	"getFrameworkCategoryMetadata from local cache" should "return touple values" in {
		//when(mockNeo4JUtil.getNodePropertiesWithObjectType(ArgumentMatchers.anyString())).thenReturn(getNeo4jData())
		enrichFrameworkMasterCategoryMap()
		val node : (List[String], Map[(String, String), List[String]]) = new TestFrameworkDataEnrichment().getFrameworkCategoryMetadata("domain", "Category")
		node._1.asInstanceOf[List[String]] should have length(6)
		node._2.asInstanceOf[Map[(String, String), List[String]]].size.equals(3) should be (true)
	}

	def getNeo4jData(): util.List[util.Map[String, AnyRef]] = {
		util.Arrays.asList(
			new util.HashMap[String, AnyRef]{{
				put("IL_UNIQUE_ID", "board")
				put("IL_FUNC_OBJECT_TYPE", "Category")
				put("code", "board")
				put("orgIdFieldName", "boardIds")
				put("targetIdFieldName", "targetBoardIds")
				put("searchIdFieldName", "se_boardIds")
				put("searchLabelFieldName", "se_boards")
			}},
			new util.HashMap[String, AnyRef]{{
				put("IL_UNIQUE_ID", "subject")
				put("IL_FUNC_OBJECT_TYPE", "Category")
				put("code", "subject")
				put("orgIdFieldName", "subjectIds")
				put("targetIdFieldName", "targetSubjectIds")
				put("searchIdFieldName", "se_subjectIds")
				put("searchLabelFieldName", "se_subjects")
			}},
			new util.HashMap[String, AnyRef]{{
				put("IL_UNIQUE_ID", "medium")
				put("IL_FUNC_OBJECT_TYPE", "Category")
				put("code", "medium")
				put("orgIdFieldName", "mediumIds")
				put("targetIdFieldName", "targetMediumIds")
				put("searchIdFieldName", "se_mediumIds")
				put("searchLabelFieldName", "se_mediums")
			}}
		)
	}

	def enrichFrameworkMasterCategoryMap() = {
		val masterCategoriesNode: util.List[util.Map[String, AnyRef]] = getNeo4jData()

		val masterCategoryMap: List[Map[String, AnyRef]] = masterCategoriesNode.asScala.map(node =>
			Map("code" -> node.getOrDefault("code", "").asInstanceOf[String],
				"orgIdFieldName" -> node.getOrDefault("orgIdFieldName", "").asInstanceOf[String],
				"targetIdFieldName" -> node.getOrDefault("targetIdFieldName", "").asInstanceOf[String],
				"searchIdFieldName" -> node.getOrDefault("searchIdFieldName", "").asInstanceOf[String],
				"searchLabelFieldName" -> node.getOrDefault("searchLabelFieldName", "").asInstanceOf[String])
		).toList
		val masterCategories: Map[String, AnyRef] = masterCategoryMap.flatMap(masterCategory => Map(masterCategory.getOrElse("code", "").asInstanceOf[String] -> masterCategory)).toMap
		FrameworkMasterCategoryMap.put("masterCategories", masterCategories)
	}
}

class TestFrameworkDataEnrichment extends FrameworkDataEnrichment {

}
