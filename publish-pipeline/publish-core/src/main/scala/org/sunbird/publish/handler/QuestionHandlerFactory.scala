package org.sunbird.publish.handler

import java.util

import com.google.gson.Gson

import scala.collection.JavaConverters._

object QuestionHandlerFactory {
    lazy private val gson = new Gson()


    private class MCQHandler extends QuestionTypeHandler {
        override def getQuestion(extData: Option[Map[String, AnyRef]]): String = {
            extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String]
        }

        override def getAnswers(extData: Option[Map[String, AnyRef]]): List[String] = {
            val responseDeclaration = gson.fromJson(extData.getOrElse(Map()).getOrElse("responseDeclaration", "").asInstanceOf[String],
                classOf[java.util.Map[String, AnyRef]]).asScala
            val valueOption: Option[Any] = responseDeclaration.getOrElse("response1", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .getOrElse("correctResponse", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .get("value")
            val answersValues: List[Double] = valueOption match {
                case Some(element: Double) => List[Double](element)
                case Some(element: util.ArrayList[Double]) => element.asScala.toList
                case _ => List()
            }
            val interactions = gson.fromJson(extData.getOrElse(Map()).getOrElse("interactions", "").asInstanceOf[String],
                classOf[java.util.Map[String, AnyRef]]).asScala

            val answers = interactions.getOrElse("response1", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .getOrElse("options", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]].asScala
                .filter(element => answersValues.contains(element.asScala.getOrElse("value", -1).asInstanceOf[Double]))
                .toList
                .map(element => element.asScala.getOrElse("body", "").asInstanceOf[String])
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
