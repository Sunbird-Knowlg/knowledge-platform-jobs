package org.sunbird.incredible.processor.qrcode

case class QRCodeGenerationModel(data: String, errorCorrectionLevel: String = "H", pixelsPerBlock: Int = 2, qrCodeMargin: Int = 3,
                                 text: String, textFontName: String = "Verdana", textFontSize: Int = 16, textCharacterSpacing: Double = 0.2,
                                 imageBorderSize: Int = 0, colorModel: String = "Grayscale", fileName: String, fileFormat: String = "png",
                                 qrCodeMarginBottom: Int = 1, imageMargin: Int = 1)