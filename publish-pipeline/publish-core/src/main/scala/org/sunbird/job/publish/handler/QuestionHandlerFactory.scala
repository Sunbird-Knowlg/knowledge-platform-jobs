package org.sunbird.job.publish.handler

import com.google.gson.Gson

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object QuestionHandlerFactory {
    lazy private val gson = new Gson()


    private class MCQHandler extends QuestionTypeHandler {
        override def getQuestion(extData: Option[Map[String, AnyRef]]): String = {
            extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String]
        }

        override def getAnswers(extData: Option[Map[String, AnyRef]]): List[String] = {
            val responseDeclaration = gson.fromJson(extData.getOrElse(Map()).getOrElse("responseDeclaration", "").asInstanceOf[String],
                classOf[java.util.Map[String, AnyRef]]).asScala
            val valueOption: Option[Any] = if(null != responseDeclaration) responseDeclaration.getOrElse("response1", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .getOrElse("correctResponse", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .get("value") else Some(AnyRef)
            val answersValues: List[Double] = valueOption match {
                case Some(element: Double) => List[Double](element)
                case Some(element: util.ArrayList[Double]) => element.asScala.toList
                case _ => List()
            }
            val interactions = gson.fromJson(extData.getOrElse(Map()).getOrElse("interactions", "").asInstanceOf[String],
                classOf[java.util.Map[String, AnyRef]]).asScala

            val answers = if(null != interactions) interactions.getOrElse("response1", new util.HashMap()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
                .getOrElse("options", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]].asScala
                .filter(element => answersValues.contains(element.asScala.getOrElse("value", -1).asInstanceOf[Double]))
                .toList
                .map(element => element.asScala.getOrElse("label", "").asInstanceOf[String]) else List()
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

    private class FTBHandler extends QuestionTypeHandler {
        override def getQuestion(extData: Option[Map[String, AnyRef]]): String =
            extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String]

        override def getAnswers(extData: Option[Map[String, AnyRef]]): List[String] = {
            val rdJson = extData.getOrElse(Map()).getOrElse("responseDeclaration", "{}").asInstanceOf[String]
            val rd = Try(gson.fromJson(rdJson, classOf[java.util.Map[String, AnyRef]]).asScala).getOrElse(mutable.Map.empty)
            rd.values.toList.flatMap { responseAny =>
                val response = responseAny.asInstanceOf[java.util.Map[String, AnyRef]].asScala
                val mapping = Try(
                    response.getOrElse("mapping", new util.ArrayList[util.Map[String, AnyRef]]())
                        .asInstanceOf[util.List[util.Map[String, AnyRef]]].asScala.toList
                ).getOrElse(List.empty)
                mapping.map(m => m.asScala.getOrElse("value", "").asInstanceOf[String]).filter(_.nonEmpty)
            }
        }
    }

    private class MTFHandler extends QuestionTypeHandler {
        override def getQuestion(extData: Option[Map[String, AnyRef]]): String =
            extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String]

        override def getAnswers(extData: Option[Map[String, AnyRef]]): List[String] = {
            val rdJson = extData.getOrElse(Map()).getOrElse("responseDeclaration", "{}").asInstanceOf[String]
            val rd = Try(gson.fromJson(rdJson, classOf[java.util.Map[String, AnyRef]]).asScala).getOrElse(mutable.Map.empty)
            val response = rd.values.headOption
                .map(_.asInstanceOf[java.util.Map[String, AnyRef]].asScala)
                .getOrElse(mutable.Map.empty)
            val correctMap = Try(
                response.getOrElse("correctResponse", new util.HashMap[String, AnyRef]())
                    .asInstanceOf[java.util.Map[String, AnyRef]].asScala
                    .getOrElse("value", new util.HashMap[String, AnyRef]())
                    .asInstanceOf[java.util.Map[String, AnyRef]].asScala
            ).getOrElse(mutable.Map.empty)
            correctMap.map { case (k, v) => s"$k:$v" }.toList
        }
    }

    private class SequenceHandler extends QuestionTypeHandler {
        override def getQuestion(extData: Option[Map[String, AnyRef]]): String =
            extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String]

        override def getAnswers(extData: Option[Map[String, AnyRef]]): List[String] = {
            val rdJson = extData.getOrElse(Map()).getOrElse("responseDeclaration", "{}").asInstanceOf[String]
            val rd = Try(gson.fromJson(rdJson, classOf[java.util.Map[String, AnyRef]]).asScala).getOrElse(mutable.Map.empty)
            val response = rd.values.headOption
                .map(_.asInstanceOf[java.util.Map[String, AnyRef]].asScala)
                .getOrElse(mutable.Map.empty)
            Try(
                response.getOrElse("correctResponse", new util.HashMap[String, AnyRef]())
                    .asInstanceOf[java.util.Map[String, AnyRef]].asScala
                    .getOrElse("value", new util.ArrayList[AnyRef]())
                    .asInstanceOf[util.List[AnyRef]].asScala.toList
                    .map(_.asInstanceOf[String])
            ).getOrElse(List.empty)
        }
    }


    def apply(questionType: Option[String]): Option[QuestionTypeHandler] = questionType match {
        case Some("Multiple Choice Question")     => Some(new MCQHandler)
        case Some("Subjective Question")          => Some(new SubjectiveHandler)
        case Some("FTB Question")                 => Some(new FTBHandler)
        case Some("Match The Following Question") => Some(new MTFHandler)
        case Some("Sequence Question")            => Some(new SequenceHandler)
        case Some("Reorder Question")             => Some(new SequenceHandler)
        case _                                    => None
    }

}
