package org.sunbird.job.qrimagegenerator.domain

case class ImageConfig(errorCorrectionLevel: String,
                       pixelsPerBlock: Int,
                       qrCodeMargin: Int,
                       textFontName: String,
                       textFontSize: Int,
                       textCharacterSpacing: Double,
                       imageFormat: String,
                       colourModel: String,
                       imageBorderSize: Int,
                       qrCodeMarginBottom: Int,
                       imageMargin: Int)

case class QRCodeImageGeneratorRequest(dialCodes: List[Map[String, AnyRef]],
                                       imageConfig: ImageConfig,
                                       tempFilePath: String)
