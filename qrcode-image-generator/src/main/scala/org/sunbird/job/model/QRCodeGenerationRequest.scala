package org.sunbird.job.model

import java.util

class QRCodeGenerationRequest {
  private var data: util.List[String] = null
  private var errorCorrectionLevel: String = null
  private var pixelsPerBlock: Int = 0
  private var qrCodeMargin: Int = 0
  private var text: util.List[String] = null
  private var textFontName: String = null
  private var textFontSize: Int = 0
  private var textCharacterSpacing: Double = .0
  private var imageBorderSize: Int = 0
  private var colorModel: String = null
  private var fileName: util.List[String] = null
  private var fileFormat: String = null
  private var qrCodeMarginBottom: Int = 0
  private var imageMargin: Int = 0
  private var tempFilePath: String = null

  def getTempFilePath: String = tempFilePath

  def setTempFilePath(tempFilePath: String): Unit = {
    this.tempFilePath = tempFilePath
  }

  def getImageMargin: Int = imageMargin

  def setImageMargin(imageMargin: Int): Unit = {
    this.imageMargin = imageMargin
  }

  def getQrCodeMarginBottom: Int = qrCodeMarginBottom

  def setQrCodeMarginBottom(qrCodeMarginBottom: Int): Unit = {
    this.qrCodeMarginBottom = qrCodeMarginBottom
  }

  def getData: util.List[String] = data

  def setData(data: util.List[String]): Unit = {
    this.data = data
  }

  def getErrorCorrectionLevel: String = errorCorrectionLevel

  def setErrorCorrectionLevel(errorCorrectionLevel: String): Unit = {
    this.errorCorrectionLevel = errorCorrectionLevel
  }

  def getPixelsPerBlock: Int = pixelsPerBlock

  def setPixelsPerBlock(pixelsPerBlock: Int): Unit = {
    this.pixelsPerBlock = pixelsPerBlock
  }

  def getQrCodeMargin: Int = qrCodeMargin

  def setQrCodeMargin(qrCodeMargin: Int): Unit = {
    this.qrCodeMargin = qrCodeMargin
  }

  def getText: util.List[String] = text

  def setText(text: util.List[String]): Unit = {
    this.text = text
  }

  def getTextFontName: String = textFontName

  def setTextFontName(textFontName: String): Unit = {
    this.textFontName = textFontName
  }

  def getTextFontSize: Int = textFontSize

  def setTextFontSize(textFontSize: Int): Unit = {
    this.textFontSize = textFontSize
  }

  def getTextCharacterSpacing: Double = textCharacterSpacing

  def setTextCharacterSpacing(textCharacterSpacing: Double): Unit = {
    this.textCharacterSpacing = textCharacterSpacing
  }

  def getImageBorderSize: Int = imageBorderSize

  def setImageBorderSize(imageBorderSize: Int): Unit = {
    this.imageBorderSize = imageBorderSize
  }

  def getColorModel: String = colorModel

  def setColorModel(colorModel: String): Unit = {
    this.colorModel = colorModel
  }

  def getFileName: util.List[String] = fileName

  def setFileName(fileName: util.List[String]): Unit = {
    this.fileName = fileName
  }

  def getFileFormat: String = fileFormat

  def setFileFormat(fileFormat: String): Unit = {
    this.fileFormat = fileFormat
  }
}

