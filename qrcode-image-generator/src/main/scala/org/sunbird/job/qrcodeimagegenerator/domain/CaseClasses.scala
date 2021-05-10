package org.sunbird.job.qrcodeimagegenerator.domain

@SerialVersionUID(-5779950964487302124L)
case class QRCodeGenerationRequest (eid: String, processId: String, dialcodes: List[Map[String,AnyRef]], config: Config, storage: StorageConfig)
case class StorageConfig(container: String, path: String, fileName: String)
case class Config(errorCorrectionLevel: Option[String] = Option(""), pixelsPerBlock: Option[Int], qrCodeMargin: Option[Int],
                  textFontName: Option[String], textFontSize: Option[Int], textCharacterSpacing: Option[Double], imageFormat: Option[String],
                  colourModel: Option[String],imageBorderSize: Option[Int], qrCodeMarginBottom: Option[Int] = Option(0), imageMargin: Option[Int])