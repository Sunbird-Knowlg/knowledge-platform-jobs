package org.sunbird.publish.spec

import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.helpers.FrameworkDataEnrichment

class FrameworkDataEnrichmentTestSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

	implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
	implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())

	override protected def beforeAll(): Unit = {
		super.beforeAll()
	}

	override protected def afterAll(): Unit = {
		super.afterAll()
	}


}

class TestFrameworkDataEnrichment extends FrameworkDataEnrichment {

}
