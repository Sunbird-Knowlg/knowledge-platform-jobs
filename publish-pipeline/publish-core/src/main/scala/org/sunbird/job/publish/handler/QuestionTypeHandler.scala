package org.sunbird.job.publish.handler

trait QuestionTypeHandler {

    def getQuestion(extData: Option[Map[String, AnyRef]]): String

    def getAnswers(extData: Option[Map[String, AnyRef]]): List[String]

}
