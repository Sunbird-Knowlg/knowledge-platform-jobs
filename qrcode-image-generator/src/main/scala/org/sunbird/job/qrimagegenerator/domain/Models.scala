package org.sunbird.job.qrimagegenerator.domain

import scala.collection.mutable.ListBuffer

case class Config(errorCorrectionLevel: Option[String] = Option(""),
                  pixelsPerBlock: Option[Int] = Option(0),
                  qrCodeMargin: Option[Int] = Option(0),
                  textFontName: Option[String] = Option(""),
                  textFontSize: Option[Int] = Option(0),
                  textCharacterSpacing: Option[Double] = Option(0),
                  imageFormat: Option[String] = Option("png"),
                  colourModel: Option[String] = Option(""),
                  imageBorderSize: Option[Int] = Option(0),
                  qrCodeMarginBottom: Option[Int] = Option(0),
                  imageMargin: Option[Int] = Option(0))

case class QRCodeImageGenerator(data: ListBuffer[String], errorCorrectionLevel: String, pixelsPerBlock: Int, qrCodeMargin: Int,
                                text: ListBuffer[String], textFontName: String, textFontSize: Int, textCharacterSpacing: Double,
                                imageBorderSize: Int, colorModel: String, fileName: ListBuffer[String], fileFormat: String,
                                qrCodeMarginBottom: Int, imageMargin: Int, tempFilePath: String)
