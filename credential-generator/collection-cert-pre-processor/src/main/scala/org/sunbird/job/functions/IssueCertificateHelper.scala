package org.sunbird.job.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Row, TypeTokens}
import org.sunbird.collectioncert.domain.Event
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.domain.{BEJobRequestEvent, EnrolledUser, EventObject}
import org.sunbird.job.task.CollectionCertPreProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, ScalaJsonUtil}

import scala.collection.JavaConverters._

trait IssueCertificateHelper {
    
    def validateTemplate(template: Map[String, String])(config: CollectionCertPreProcessorConfig):Map[String, AnyRef] = {
        val criteria = ScalaJsonUtil.deserialize[Map[String, AnyRef]](template.getOrElse(config.criteria, "{}"))
        if(!template.getOrElse("url", "").isEmpty && !criteria.isEmpty && !criteria.keySet.intersect(Set(config.enrollment, config.assessment, config.users)).isEmpty) {
            criteria
        } else {
            throw new Exception("Invalid template")
        }
    }

    def validateEnrolmentCriteria(event: Event, enrollmentCriteria: Map[String, AnyRef], certName: String)(metrics:Metrics, cassandraUtil: CassandraUtil, config:CollectionCertPreProcessorConfig): EnrolledUser = {
        if(!enrollmentCriteria.isEmpty) {
            val query = QueryBuilder.select("active", "issued_certificates", "completedon", "status").from(config.keyspace, config.userEnrolmentsTable)
              .where(QueryBuilder.eq(config.dbUserId, event.userId)).and(QueryBuilder.eq(config.dbCourseId, event.courseId))
              .and(QueryBuilder.eq(config.dbBatchId, event.batchId))
            val row = cassandraUtil.findOne(query.toString)
            metrics.incCounter(config.dbReadCount)
            if(null != row){
                val active:Boolean = row.getBool(config.active)   
                val issuedCertificates = row.getList(config.issuedCertificates, TypeTokens.mapOf(classOf[String], classOf[String])).asScala.toList
                val isCertIssued = !issuedCertificates.isEmpty && !issuedCertificates.filter(cert => certName.equalsIgnoreCase(cert.getOrDefault(config.name,"").asInstanceOf[String])).isEmpty
                val status = row.getInt(config.status)
                val criteriaStatus = enrollmentCriteria.getOrElse(config.status, 2)
                val oldId = if(isCertIssued && event.reIssue) issuedCertificates.filter(cert => certName.equalsIgnoreCase(cert.getOrDefault(config.name,"").asInstanceOf[String]))
                  .map(cert => cert.getOrDefault(config.identifier, "")).head else ""
                val userId = if(active && (criteriaStatus == status) && (!isCertIssued || event.reIssue)) event.userId else ""
                val issuedOn = row.getTimestamp(config.completedOn)
                EnrolledUser(userId, oldId, issuedOn)
            } else EnrolledUser("", "")
        } else EnrolledUser(event.userId, "") 
    }

    def getMaxScore(event: Event)(metrics:Metrics, cassandraUtil: CassandraUtil, config:CollectionCertPreProcessorConfig):Double = {
        val query = QueryBuilder.select().column("total_max_score").max("total_score").as("score").from(config.keyspace, config.assessmentTable)
          .where(QueryBuilder.eq("course_id", event.courseId)).and(QueryBuilder.eq("batch_id", event.batchId))
          .and(QueryBuilder.eq("user_id", event.userId)).groupBy("user_id", "course_id", "batch_id", "content_id")
        
        val rows: java.util.List[Row] = cassandraUtil.find(query.toString)
        metrics.incCounter(config.dbReadCount)
        if(null != rows && !rows.isEmpty) {
            rows.asScala.toList.map(row => ((row.getDouble("score")*100)/row.getDouble("total_max_score"))).toList.max
        } else 0d
    }

    def isValidAssessCriteria(assessmentCriteria: Map[String, AnyRef], score: Double): Boolean = {
        if(assessmentCriteria.get("score").isInstanceOf[Number]) {
            score == assessmentCriteria.get("score").asInstanceOf[Int].toDouble
        } else {
            val scoreCriteria = assessmentCriteria.getOrElse("score", Map[String, AnyRef]()).asInstanceOf[Map[String, Int]]
            if(scoreCriteria.isEmpty) false
            else {
                val operation = scoreCriteria.head._1
                val criteriaScore = scoreCriteria.head._2.toDouble
                operation match {
                    case "EQ" => score == criteriaScore
                    case "eq" => score == criteriaScore
                    case "=" => score == criteriaScore
                    case ">" => score > criteriaScore
                    case "<" => score < criteriaScore
                    case ">=" => score >= criteriaScore
                    case "<=" => score <= criteriaScore
                    case "ne" => score != criteriaScore
                    case "!=" => score != criteriaScore
                    case _ => false
                }
            }
            
        }
    }

    def validateAssessmentCriteria(event: Event, assessmentCriteria: Map[String, AnyRef], enrolledUser: String)(metrics:Metrics, cassandraUtil: CassandraUtil, config:CollectionCertPreProcessorConfig):String = {
        var assessedUser = enrolledUser
        
        if(!assessmentCriteria.isEmpty && !enrolledUser.isEmpty) {
            val score:Double = getMaxScore(event)(metrics, cassandraUtil, config)
            if(!isValidAssessCriteria(assessmentCriteria, score)) 
                assessedUser = ""
        }
        assessedUser   
    }

    def validateUser(userId: String, map: Map[String, AnyRef])(metrics:Metrics, config:CollectionCertPreProcessorConfig, httpUtil: HttpUtil) = {
        if(!userId.isEmpty) {
            val url = config.learnerBasePath + config.userReadApi + "/" + userId
            val result = getAPICall(url, "response")(config, httpUtil)
            result
        } else Map[String, AnyRef]()
        
    }
    
    def getAPICall(url: String, responseParam: String)(config:CollectionCertPreProcessorConfig, httpUtil: HttpUtil): Map[String,AnyRef] = {
        val response = httpUtil.get(url, config.defaultHeaders)
        if(200 == response.status) {
            ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
              .getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
              .getOrElse(responseParam, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        } else {
            Map[String,AnyRef]()
        }
    }

    def getCourseName(courseId: String)(metrics:Metrics, config:CollectionCertPreProcessorConfig, cache:DataCache, httpUtil: HttpUtil) = {
        val courseMetadata = cache.getWithRetry(courseId)
        if(!courseMetadata.isEmpty) {
            val url = config.contentBasePath + config.contentReadApi + "/" + courseId + "?fields=name"
            val response = getAPICall(url, "content")(config, httpUtil)
            response.getOrElse(config.name, "")
        } else {
            courseMetadata.getOrElse(config.name, "")
        }
    }

    def generateCertificateEvent(event: Event, template: Map[String, String], userDetails: Map[String, AnyRef], enrolledUser: EnrolledUser, certName: String)(metrics:Metrics, config:CollectionCertPreProcessorConfig, cache:DataCache, httpUtil: HttpUtil) = {
        val firstName = userDetails.getOrElse("firstName", "").asInstanceOf[String]
        val lastName = userDetails.getOrElse("lastName", "").asInstanceOf[String]
        def nullStringCheck(name:String) = {if(!"null".equalsIgnoreCase(name)) name  else ""}
        val recipientName = (nullStringCheck(firstName) + " " + nullStringCheck(lastName)).trim
        val courseName = getCourseName(event.courseId)(metrics, config, cache, httpUtil)
        val eData = Map[String, AnyRef] (
            "issuedOn" -> enrolledUser.issuedOn,
            "data" -> List(Map[String, AnyRef]("recipientName" -> recipientName, "recipientId" -> event.userId)),
            "criteria" -> Map[String, String]("narrative" -> event.courseId),
            "svgTemplate" -> template.getOrElse("url", ""),
            "oldId" -> enrolledUser.oldId,
            "templateId" -> template.getOrElse(config.identifier, ""),
            "userId" -> event.userId,
            "orgId" -> userDetails.getOrElse("rootOrgId", ""),
            "issuer" -> template.getOrElse(config.issuer, ""),
            "signatoryList" -> template.getOrElse(config.signatoryList, ""),
            "courseName" -> courseName,
            "basePath" -> config.certBasePath,
            "related" ->  Map[String, AnyRef]("batchId" -> event.batchId, "courseId" -> event.courseId, "type" -> certName),
            "name" -> certName,
            "tag" -> event.batchId
        )

        ScalaJsonUtil.serialize(BEJobRequestEvent(edata = eData, `object` = EventObject(id= event.userId)))
    }

    def issueCertificate(event:Event, template: Map[String, String])(cassandraUtil: CassandraUtil, cache:DataCache, metrics: Metrics, config: CollectionCertPreProcessorConfig, httpUtil: HttpUtil): String = {
        //validCriteria
        val criteria = validateTemplate(template)(config)
        //validateEnrolmentCriteria
        val certName = template.getOrElse(config.name, "")
        val enrolledUser: EnrolledUser = validateEnrolmentCriteria(event, criteria.getOrElse(config.enrollment, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]], certName)(metrics, cassandraUtil, config)
        //validateAssessmentCriteria
        val assessedUser = validateAssessmentCriteria(event, criteria.getOrElse(config.assessment, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]], enrolledUser.userId)(metrics, cassandraUtil, config)
        //validateUserCriteria
        val userDetails = validateUser(assessedUser, criteria.getOrElse(config.user, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]])(metrics, config, httpUtil)
        
        //generateCertificateEvent
        if(!userDetails.isEmpty) {
            generateCertificateEvent(event, template, userDetails, enrolledUser, certName)(metrics, config, cache, httpUtil)
        } else throw new Exception(s"""User :: ${event.userId} did not match the criteria for batch :: ${event.batchId} and course :: ${event.courseId}""")
    }

}
