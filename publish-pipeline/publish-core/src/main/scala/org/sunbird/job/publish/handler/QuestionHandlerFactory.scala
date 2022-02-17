package org.sunbird.job.publish.handler

import com.google.gson.Gson
import java.util

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object QuestionHandlerFactory {
    lazy private val gson = new Gson()


    private class MCQHandler extends QuestionTypeHandler {
        private[this] val logger = LoggerFactory.getLogger(classOf[MCQHandler])
        override def getQuestion(extData: Option[Map[String, AnyRef]]): String = {
            logger.info("D:: MCQHandler :: getQuestion :: extData :: "+extData)
            extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String]
        }

        override def getAnswers(extData: Option[Map[String, AnyRef]]): List[String] = {
            logger.info("D:: MCQHandler :: getAnswers :: start")
            val responseDeclaration = gson.fromJson(extData.getOrElse(Map()).getOrElse("responseDeclaration", "").asInstanceOf[String],
                classOf[java.util.Map[String, AnyRef]]).asScala
            logger.info("D:: MCQHandler :: getAnswers :: responseDeclaration :: "+responseDeclaration)
            val valueOption: Option[Any] = if(null != responseDeclaration) responseDeclaration.getOrElse("response1", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .getOrElse("correctResponse", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .get("value") else Some(AnyRef)
            logger.info("D:: MCQHandler :: getAnswers :: valueOption :: "+valueOption)
            val answersValues: List[Double] = valueOption match {
                case Some(element: Double) => List[Double](element)
                case Some(element: util.ArrayList[Double]) => element.asScala.toList
                case _ => List()
            }
            logger.info("D:: MCQHandler :: getAnswers :: answersValues :: "+answersValues)
            val interactions = gson.fromJson(extData.getOrElse(Map()).getOrElse("interactions", "").asInstanceOf[String],
                classOf[java.util.Map[String, AnyRef]]).asScala
            logger.info("D:: MCQHandler :: getAnswers :: interactions :: "+interactions)

            val answers = if(null != interactions) interactions.getOrElse("response1", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .getOrElse("options", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]].asScala
                .filter(element => answersValues.contains(element.asScala.getOrElse("value", -1).asInstanceOf[Double]))
                .toList
                .map(element => element.asScala.getOrElse("label", "").asInstanceOf[String]) else List()
            logger.info("D:: MCQHandler :: getAnswers :: answers :: "+answers)
            answers
        }
    }

    private class SubjectiveHandler extends QuestionTypeHandler {
        override def getQuestion(extData: Option[Map[String, AnyRef]]): String = {
            extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String]

        }

        override def getAnswers(extData: Option[Map[String, AnyRef]]): List[String] = {
            List(extData.getOrElse(Map()).getOrElse("answer", "").asInstanceOf[String])
        }
    }


    def apply(questionType: Option[String]): Option[QuestionTypeHandler] = questionType match {
        case Some("Multiple Choice Question") => Some(new MCQHandler)
        case Some("Subjective Question") => Some(new SubjectiveHandler)
        case Some("FTB Question") => None
        case _ => None
    }

}
