package org.sunbird.job.collectioncert.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

import java.util

class CollectionCertPreProcessorConfig(override val config: Config) extends BaseJobConfig(config, "collection-cert-pre-processor") {

    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    
    //Redis config
    val collectionCacheStore: Int = 0
    val contentCacheStore: Int = 5
    val metaRedisHost: String = config.getString("redis-meta.host")
    val metaRedisPort: Int = config.getInt("redis-meta.port")

    
    //kafka config
    val kafkaInputTopic: String = config.getString("kafka.input.topic")
    val kafkaOutputTopic: String = config.getString("kafka.output.topic")
    val certificatePreProcessorConsumer: String = "collection-cert-pre-processor-consumer"
    val generateCertificateProducer = "generate-certificate-sink"
    override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
    val generateCertificateParallelism:Int = config.getInt("task.generate_certificate.parallelism")
    
    //Tags
    val generateCertificateOutputTagName = "generate-certificate-request"
    val generateCertificateOutputTag: OutputTag[String] = OutputTag[String](generateCertificateOutputTagName)

    //Cassandra config
    val dbHost: String = config.getString("lms-cassandra.host")
    val dbPort: Int = config.getInt("lms-cassandra.port")
    val keyspace: String = config.getString("lms-cassandra.keyspace")
    val courseTable: String = config.getString("lms-cassandra.course_batch.table")
    val userEnrolmentsTable: String = config.getString("lms-cassandra.user_enrolments.table")
    val assessmentTable: String = config.getString("lms-cassandra.assessment_aggregator.table")
    val useActivityAggTable: String = config.getString("lms-cassandra.user_activity_agg.table")
    val dbBatchId = "batchid"
    val dbCourseId = "courseid"
    val dbUserId = "userid"
    
    //API URL
    val contentBasePath = config.getString("service.content.basePath")
    val learnerBasePath = config.getString("service.learner.basePath")
    val userReadApi = config.getString("user_read_api")
    val contentReadApi = config.getString("content_read_api")

    // Metric List
    val totalEventsCount = "total-events-count"
    val successEventCount = "success-events-count"
    val failedEventCount = "failed-events-count"
    val skippedEventCount = "skipped-event-count"
    val dbReadCount = "db-read-count"
    val dbUpdateCount = "db-update-count"
    val cacheHitCount = "cache-hit-cout"
    
    //Constants
    val issueCertificate = "issue-certificate"
    val certTemplates = "cert_templates"
    val criteria: String = "criteria"
    val enrollment: String = "enrollment"
    val assessment: String = "assessment"
    val users: String = "users"
    val active: String = "active"
    val issuedCertificates: String = "issued_certificates"
    val status: String = "status"
    val name: String = "name"
    val user: String= "user"
    val defaultHeaders = Map[String, String] ("Content-Type" -> "application/json")
    val identifier: String = "identifier"
    val completedOn: String = "completedon"
    val issuer: String = "issuer"
    val signatoryList: String = "signatoryList"
    val certBasePath: String = config.getString("cert_domain_url") + "/certs"
    val assessmentContentTypes = if(config.hasPath("assessment.metrics.supported.contenttype")) config.getStringList("assessment.metrics.supported.contenttype") else util.Arrays.asList("SelfAssess")
    val userAccBlockedErrCode = "UOS_USRRED0006"

}
