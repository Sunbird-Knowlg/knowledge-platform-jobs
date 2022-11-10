package org.sunbird.job.cspmigrator.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.cspmigrator.helpers.{CSPCassandraMigrator, CSPNeo4jMigrator}
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.util._

import scala.collection.JavaConverters._

class CSPMigratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: CSPMigratorConfig = new CSPMigratorConfig(config)
  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  var cassandraUtil: CassandraUtil = _
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(jobConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  "ECML Body" should " get updated with migrate data in cassandra database" in {

    val cspneo4jMigrator = new TestCSPNeo4jMigrator()
    val fieldsToMigrate: List[String] = jobConfig.getConfig.getStringList("neo4j_fields_to_migrate.content").asScala.toList

    val objectMetadata = Map[String, AnyRef]("ownershipType" -> Array("createdFor"), "previewUrl" -> "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/ecml/do_31270597860728832015700-latest",
      "keywords" -> Array("10 PS BITS"),
      "channel" -> "0123207707019919361056",
      "downloadUrl" -> "https://ntpproductionall.blob.core.windows.net/ntp-content-production/ecar_files/do_31270597860728832015700/examprep_10tm_ps_cha-4-q5_1551025589101_do_31270597860728832015700_1.0.ecar",
      "mimeType" -> "application/vnd.ekstep.ecml-archive",
      "variants" -> Map[String,AnyRef](
        "spine" -> Map[String,AnyRef](
          "ecarUrl" -> "https://ntpproductionall.blob.core.windows.net/ntp-content-production/ecar_files/do_31270597860728832015700/examprep_10tm_ps_cha-4-q5_1551025590393_do_31270597860728832015700_1.0_spine.ecar",
          "size" -> 8820.asInstanceOf[Number]
        )),
      "editorState" -> "{\"plugin\":{\"noOfExtPlugins\":14,\"extPlugins\":[{\"plugin\":\"org.ekstep.contenteditorfunctions\",\"version\":\"1.2\"},{\"plugin\":\"org.ekstep.keyboardshortcuts\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.richtext\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.iterator\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.navigation\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.mathtext\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.libs.ckeditor\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.questionunit\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.keyboard\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.questionunit.mcq\",\"version\":\"1.1\"},{\"plugin\":\"org.ekstep.questionunit.mtf\",\"version\":\"1.1\"},{\"plugin\":\"org.ekstep.questionunit.reorder\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.questionunit.sequence\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.questionunit.ftb\",\"version\":\"1.0\"}]},\"stage\":{\"noOfStages\":2,\"currentStage\":\"b8b47094-1d69-43a1-9c88-c02e760996c5\"},\"sidebar\":{\"selectedMenu\":\"settings\"}}",
      "objectType" -> "Content",
      "appIcon" -> "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31270597860728832015700/artifact/10-ps-tm_1550378309450.thumb.png",
      "primaryCategory" -> "Learning Resource",
      "artifactUrl" -> "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31270597860728832015700/artifact/1551025588108_do_31270597860728832015700.zip",
      "contentType" -> "Resource",
      "identifier" -> "do_31270597860728832015700",
      "visibility" -> "Default",
      "author" -> "APEKX",
      "lastPublishedBy" -> "8587c52d-755b-4473-8d47-4b47f81eb56b",
      "version" -> 2.asInstanceOf[Number],
      "license" -> "CC BY 4.0",
      "prevState" -> "Review",
      "size" -> 9312393.asInstanceOf[Number],
      "lastPublishedOn" -> "2019-02-24T16 ->26 ->29.101+0000",
      "name" -> "\tExamprep_10tm_ps_cha 4-Q5",
      "status" -> "Live",
      "totalQuestions" -> 1.asInstanceOf[Number],
      "code" -> "org.sunbird.zRuXiC",
      "description" -> "10 PS BITS",
      "streamingUrl" -> "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/ecml/do_31270597860728832015700-latest",
      "posterImage" -> "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31270069910858956813856/artifact/10-ps-tm_1550378309450.png",
      "idealScreenSize" -> "normal",
      "createdOn" -> "2019-02-24T15:39:39.209+0000",
      "copyrightYear" -> 2019.asInstanceOf[Number],
      "contentDisposition" -> "inline",
      "lastUpdatedOn" -> "2019-02-24T16:26:24.241+0000",
      "dialcodeRequired" -> "No",
      "owner" -> "IT_CELL CSE_AP AMARAVATI",
      "creator" -> "MALLIMOGGALA SUBBAYYA",
      "totalScore" -> 1.asInstanceOf[Number],
      "pkgVersion" -> 1.asInstanceOf[Number],
      "versionKey" -> "1551025584241",
      "idealScreenDensity" -> "hdpi",
      "framework" -> "ap_k-12_1",
      "s3Key" -> "ecar_files/do_31270597860728832015700/examprep_10tm_ps_cha-4-q5_1551025589101_do_31270597860728832015700_1.0.ecar",
      "lastSubmittedOn" -> "2019-02-24T15 ->44:04.741+0000",
      "compatibilityLevel" -> 4.asInstanceOf[Number],
      "ownedBy" -> "01232154589404364810952",
      "board" -> "State (Andhra Pradesh)",
      "resourceType" -> "Learn"
    )


    val cspCassandraMigrator = new TestCSPCassandraMigrator()

    cspCassandraMigrator.process(objectMetadata, "Draft", jobConfig, mockHttpUtil, cassandraUtil, cloudStorageUtil)

    when(mockHttpUtil.getSize(anyString(), any())).thenReturn(200)

    val migratedMetadata = cspneo4jMigrator.process(objectMetadata, "Draft", jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil)
    fieldsToMigrate.map(migrateField => {
      jobConfig.keyValueMigrateStrings.keySet().toArray().map(key => {
        assert(!migratedMetadata.getOrElse(migrateField, "").asInstanceOf[String].contains(key))
      })
    })
  }

  "Collection Hierarchy" should " get updated with migrate data in cassandra database" in {
    val cspMigrator = new TestCSPNeo4jMigrator()

    val objectMetadata = Map[String, AnyRef](
      "ownershipType" -> Array(
      "createdBy"
      ),
    "copyright" -> "tn",
    "se_gradeLevelIds" -> Array(
    "tn_k-12_5_gradelevel_class2"
    ),
    "keywords" -> Array(
    "sandhya"
    ),
    "subject" -> Array(
    "Accounting And Auditing"
    ),
    "targetMediumIds" -> Array(
    "tn_k-12_5_medium_english",
    "tn_k-12_5_medium_tamil"
    ),
    "channel" -> "01269878797503692810",
    "downloadUrl" -> "https://sunbirdstagingpublic.blob.core.windows.net/sunbird-content-staging/content/do_21351537329604198413739/4.8.1-rc-end-to-end-verification_1649825926657_do_21351537329604198413739_1_SPINE.ecar",
    "mimeType" -> "application/vnd.ekstep.content-collection",
    "variants" -> Map[String, AnyRef] (
      "spine" -> Map[String, AnyRef] (
        "ecarUrl" -> "https://sunbirdstagingpublic.blob.core.windows.net/sunbird-content-staging/content/do_21351537329604198413739/4.8.1-rc-end-to-end-verification_1649825926657_do_21351537329604198413739_1_SPINE.ecar",
        "size" -> "42727"
      ),
      "online" -> Map[String, AnyRef] (
        "ecarUrl" -> "https://sunbirdstagingpublic.blob.core.windows.net/sunbird-content-staging/content/do_21351537329604198413739/4.8.1-rc-end-to-end-verification_1649825926838_do_21351537329604198413739_1_ONLINE.ecar",
        "size" -> "7931"
      )
    ),
    "leafNodes" -> Array(
    "do_21339794950117785611",
    "do_2131289236157972481377"
    ),
    "targetGradeLevelIds" -> Array(
    "tn_k-12_5_gradelevel_class2"
    ),
    "objectType" -> "Content",
    "se_mediums" -> Array(
    "English",
    "Tamil"
    ),
    "primaryCategory" -> "Course",
    "appId" -> "staging.sunbird.portal",
    "contentEncoding" -> "gzip",
    "lockKey" -> "63d3921f-defc-4838-b465-6ca222f5d180",
    "generateDIALCodes" -> "No",
    "totalCompressedSize" -> 804725.asInstanceOf[Number],
    "mimeTypesCount" -> "{\"application/vnd.ekstep.ecml-archive\":2,\"application/vnd.ekstep.content-collection\":1}",
    "sYS_INTERNAL_LAST_UPDATED_ON" -> "2022-04-13T05:00:21.380+0000",
    "contentType" -> "Course",
    "se_gradeLevels" -> Array(
    "Class 2"
    ),
    "identifier" -> "do_21351537329604198413739",
    "se_boardIds" -> Array(
    "tn_k-12_5_board_statetamilnadu"
    ),
    "subjectIds" -> Array(
    "tn_k-12_5_subject_accountingandauditing"
    ),
    "toc_url" -> "https://sunbirdstagingpublic.blob.core.windows.net/sunbird-content-staging/content/do_21351537329604198413739/artifact/do_21351537329604198413739_toc.json",
    "visibility" -> "Default",
    "contentTypesCount" -> "{\"PracticeResource\":1,\"SelfAssess\":1,\"CourseUnit\":1}",
    "author" -> "newtncc guy",
    "consumerId" -> "cb069f8d-e4e1-46c5-831f-d4a83b323ada",
    "childNodes" -> Array(
    "do_21339794950117785611",
    "do_21351537331811942413741",
    "do_2131289236157972481377"
    ),
    "discussionForum" -> {
      "enabled" -> "Yes"
    },
    "mediaType" -> "content",
    "osId" -> "org.ekstep.quiz.app",
    "lastPublishedBy" -> "08631a74-4b94-4cf7-a818-831135248a4a",
    "version" -> 2.asInstanceOf[Number],
    "se_subjects" -> Array(
    "Accounting And Auditing"
    ),
    "license" -> "CC BY 4.0",
    "prevState" -> "Review",
    "size" -> 42727.asInstanceOf[Number],
    "lastPublishedOn" -> "2022-04-13T04:58:46.167+0000",
    "name" -> " 4.8.1 RC end to end verification ",
    "targetBoardIds" -> Array(
    "tn_k-12_5_board_statetamilnadu"
    ),
    "status" -> "Live",
    "code" -> "org.sunbird.nldxzf.copy.copy",
    "credentials" -> {
      "enabled" -> "Yes"
    },
    "prevStatus" -> "Processing",
    "origin" -> "do_21351486882984755213610",
    "description" -> "Enter description for Course",
    "idealScreenSize" -> "normal",
    "createdOn" -> "2022-04-13T04:57:21.808+0000",
    "reservedDialcodes" -> Map[String, AnyRef] (
      "J3U7Y2" -> 0.asInstanceOf[Number]
    ),
    "se_boards" -> Array(
    "State (Tamil Nadu)"
    ),
    "targetSubjectIds" -> Array(
    "tn_k-12_5_subject_mathematics"
    ),
    "se_mediumIds" -> Array(
    "tn_k-12_5_medium_english",
    "tn_k-12_5_medium_tamil"
    ),
    "copyrightYear" -> 2022.asInstanceOf[Number],
    "contentDisposition" -> "inline",
    "additionalCategories" -> Array(
    "Lesson Plan",
    "Textbook"
    ),
    "dialcodeRequired" -> "No",
    "lastStatusChangedOn" -> "2022-04-13T04:58:46.915+0000",
    "createdFor" -> Array(
    "01269878797503692810",
    "01275630321992499239077"
    ),
    "creator" -> "Guest name changed",
    "se_subjectIds" -> Array(
    "tn_k-12_5_subject_accountingandauditing",
    "tn_k-12_5_subject_mathematics"
    ),
    "se_FWIds" -> Array(
    "tn_k-12_5"
    ),
    "targetFWIds" -> Array(
    "tn_k-12_5"
    ),
    "pkgVersion" -> 1.asInstanceOf[Number],
    "versionKey" -> "1649825885342",
    "idealScreenDensity" -> "hdpi",
    "framework" -> "tn_k-12_5",
    "dialcodes" -> Array("J3U7Y2"),
    "depth" -> 0.asInstanceOf[Number],
    "s3Key" -> "content/do_21351537329604198413739/artifact/do_21351537329604198413739_toc.json",
    "lastSubmittedOn" -> "2022-04-13T04:58:05.031+0000",
    "createdBy" -> "fca2925f-1eee-4654-9177-fece3fd6afc9",
    "compatibilityLevel" -> 4.asInstanceOf[Number],
    "leafNodesCount" -> 2.asInstanceOf[Number],
    "userConsent" -> "Yes",
    "resourceType" -> "Course"
    )

    when(mockHttpUtil.getSize(anyString(), any())).thenReturn(200)
    val fieldsToMigrate: List[String] = jobConfig.getConfig.getStringList("neo4j_fields_to_migrate.collection").asScala.toList
    val migratedMetadata = cspMigrator.process(objectMetadata, "Draft", jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil)
    fieldsToMigrate.map(migrateField => {
      jobConfig.keyValueMigrateStrings.keySet().toArray().map(key => {
        assert(!migratedMetadata.getOrElse(migrateField, "").asInstanceOf[String].contains(key))
      })
    })
  }

}

class TestCSPNeo4jMigrator extends CSPNeo4jMigrator {}

class TestCSPCassandraMigrator extends CSPCassandraMigrator {}
