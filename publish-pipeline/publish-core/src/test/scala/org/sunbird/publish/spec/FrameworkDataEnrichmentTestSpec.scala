package org.sunbird.publish.spec

import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.helpers.FrameworkDataEnrichment

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
}

class TestFrameworkDataEnrichment extends FrameworkDataEnrichment {

}
