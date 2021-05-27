package org.sunbird.collectioncert.domain

import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long)  extends JobRequest(eventMap, partition, offset) {
    
    def action:String = readOrDefault[String]("edata.action", "")

    def batchId: String = readOrDefault[String]("edata.batchId", "")

    def courseId: String = readOrDefault[String]("edata.courseId", "")

    def userId: String = {
        val list = readOrDefault[List[String]]("edata.userIds", List[String]())
        if(list.isEmpty) "" else list.head
    }
    
    def reIssue: Boolean = readOrDefault[Boolean]("edata.reIssue", false)

    def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata", Map[String, AnyRef]())

}
