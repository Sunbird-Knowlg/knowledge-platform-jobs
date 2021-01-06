package org.sunbird.incredible.processor.qrcode

import java.awt.{Color, Font, FontFormatException, FontMetrics, Graphics2D, RenderingHints}
import java.awt.font.TextAttribute
import java.awt.image.BufferedImage
import java.io.{File, IOException, InputStream}
import java.util

import com.google.zxing.{BarcodeFormat, EncodeHintType, NotFoundException, WriterException}
import com.google.zxing.client.j2se.BufferedImageLuminanceSource
import com.google.zxing.common.{BitMatrix, HybridBinarizer}
import com.google.zxing.qrcode.QRCodeWriter
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel
import javax.imageio.ImageIO
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

class QRCodeImageGenerator {

  private val logger: Logger = LoggerFactory.getLogger(classOf[QRCodeImageGenerator])
  private val qrCodeWriter: QRCodeWriter = new QRCodeWriter

  @throws[WriterException]
  @throws[IOException]
  @throws[NotFoundException]
  @throws[FontFormatException]
  def createQRImages(qrGenRequest: QRCodeGenerationModel): File = {
    val data = qrGenRequest.data
    val text = qrGenRequest.text
    val fileName = qrGenRequest.fileName
    val errorCorrectionLevel = qrGenRequest.errorCorrectionLevel
    val pixelsPerBlock = qrGenRequest.pixelsPerBlock
    val qrMargin = qrGenRequest.qrCodeMargin
    val fontName = qrGenRequest.textFontName
    val fontSize = qrGenRequest.textFontSize
    val tracking = qrGenRequest.textCharacterSpacing
    val imageFormat = qrGenRequest.fileFormat
    val colorModel = qrGenRequest.colorModel
    val borderSize = qrGenRequest.imageBorderSize
    val qrMarginBottom = qrGenRequest.qrCodeMargin
    val imageMargin = qrGenRequest.imageMargin

    var qrImage: BufferedImage = generateBaseImage(data, errorCorrectionLevel, pixelsPerBlock, qrMargin, colorModel)

    if (StringUtils.isNotBlank(text)) {
      val textImage: BufferedImage = getTextImage(text, fontName, fontSize, tracking, colorModel)
      qrImage = addTextToBaseImage(qrImage, textImage, colorModel, qrMargin, pixelsPerBlock, qrMarginBottom, imageMargin)
    }

    if (borderSize > 0) drawBorder(qrImage, borderSize, imageMargin)
    val finalImageFile = new File(fileName + "." + imageFormat)
    ImageIO.write(qrImage, imageFormat, finalImageFile)
    logger.info("qr created")
    finalImageFile
  }

  @throws[NotFoundException]
  private def addTextToBaseImage(qrImage: BufferedImage, textImage: BufferedImage, colorModel: String, qrMargin: Int, pixelsPerBlock: Int, qrMarginBottom: Int, imageMargin: Int): BufferedImage = {
    val qrSource: BufferedImageLuminanceSource = new BufferedImageLuminanceSource(qrImage)
    val qrBinarizer: HybridBinarizer = new HybridBinarizer(qrSource)
    var qrBits: BitMatrix = qrBinarizer.getBlackMatrix

    val textSource: BufferedImageLuminanceSource = new BufferedImageLuminanceSource(textImage)
    val textBinarizer: HybridBinarizer = new HybridBinarizer(textSource)
    var textBits: BitMatrix = textBinarizer.getBlackMatrix

    if (qrBits.getWidth > textBits.getWidth) {
      val tempTextMatrix: BitMatrix = new BitMatrix(qrBits.getWidth, textBits.getHeight)
      copyMatrixDataToBiggerMatrix(textBits, tempTextMatrix)
      textBits = tempTextMatrix
    }
    else if (qrBits.getWidth < textBits.getWidth) {
      val tempQrMatrix: BitMatrix = new BitMatrix(textBits.getWidth, qrBits.getHeight)
      copyMatrixDataToBiggerMatrix(qrBits, tempQrMatrix)
      qrBits = tempQrMatrix
    }
    val mergedMatrix = mergeMatricesOfSameWidth(qrBits, textBits, qrMargin, pixelsPerBlock, qrMarginBottom, imageMargin)
    getImage(mergedMatrix, colorModel)
  }

  @throws[WriterException]
  private def generateBaseImage(data: String, errorCorrectionLevel: String, pixelsPerBlock: Int, qrMargin: Int, colorModel: String) = {
    val hintsMap = getHintsMap(errorCorrectionLevel, qrMargin)
    val defaultBitMatrix: BitMatrix = getDefaultBitMatrix(data, hintsMap)
    val largeBitMatrix: BitMatrix = getBitMatrix(data, defaultBitMatrix.getWidth * pixelsPerBlock, defaultBitMatrix.getHeight * pixelsPerBlock, hintsMap)
    val qrImage: BufferedImage = getImage(largeBitMatrix, colorModel)
    qrImage
  }

  //To remove extra spaces between text and qrcode, margin below qrcode is removed
  //Parameter, qrCodeMarginBottom, is introduced to add custom margin(in pixels) between qrcode and text
  //Parameter, imageMargin is introduced, to add custom margin(in pixels) outside the black border of the image
  private def mergeMatricesOfSameWidth(firstMatrix: BitMatrix, secondMatrix: BitMatrix, qrMargin: Int, pixelsPerBlock: Int, qrMarginBottom: Int, imageMargin: Int) = {
    val mergedWidth: Int = firstMatrix.getWidth + (2 * imageMargin)
    val mergedHeight: Int = firstMatrix.getHeight + secondMatrix.getHeight + (2 * imageMargin)
    val defaultBottomMargin: Int = pixelsPerBlock * qrMargin
    val marginToBeRemoved: Int = if (qrMarginBottom > defaultBottomMargin) 0 else defaultBottomMargin - qrMarginBottom
    val mergedMatrix: BitMatrix = new BitMatrix(mergedWidth, mergedHeight - marginToBeRemoved)
    for (x <- 0 until firstMatrix.getWidth;
         y <- 0 until firstMatrix.getHeight - marginToBeRemoved
         if firstMatrix.get(x, y)) {
      mergedMatrix.set(x + imageMargin, y + imageMargin)
    }
    for (x <- 0 until secondMatrix.getWidth;
         y <- 0 until secondMatrix.getHeight if secondMatrix.get(x, y)) {
      mergedMatrix.set(
        x + imageMargin,
        y + firstMatrix.getHeight - marginToBeRemoved + imageMargin)
    }
    mergedMatrix
  }

  private def copyMatrixDataToBiggerMatrix(fromMatrix: BitMatrix, toMatrix: BitMatrix): Unit = {
    val widthDiff: Int = toMatrix.getWidth - fromMatrix.getWidth
    val leftMargin: Int = widthDiff / 2
    for (x <- 0 until fromMatrix.getWidth; y <- 0 until fromMatrix.getHeight
         if fromMatrix.get(x, y)) {
      toMatrix.set(x + leftMargin, y)
    }
  }

  private def drawBorder(image: BufferedImage, borderSize: Int, imageMargin: Int): Unit = {
    image.createGraphics
    val graphics = image.getGraphics.asInstanceOf[Graphics2D]
    graphics.setColor(Color.BLACK)
    var i = 0
    for (i <- 0 until borderSize) {
      graphics.drawRect(i + imageMargin, i + imageMargin, image.getWidth - 1 - (2 * i) - (2 * imageMargin), image.getHeight - 1 - (2 * i) - (2 * imageMargin))
    }
    graphics.dispose()
  }

  private def getImage(bitMatrix: BitMatrix, colorModel: String) = {
    val imageWidth = bitMatrix.getWidth
    val imageHeight = bitMatrix.getHeight
    val image = new BufferedImage(imageWidth, imageHeight, getImageType(colorModel))
    image.createGraphics
    val graphics = image.getGraphics.asInstanceOf[Graphics2D]
    graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF)
    graphics.setColor(Color.WHITE)
    graphics.fillRect(0, 0, imageWidth, imageHeight)
    graphics.setColor(Color.BLACK)

    for (i <- 0 until imageWidth; j <- 0 until imageHeight
         if bitMatrix.get(i, j)) {
      graphics.fillRect(i, j, 1, 1)
    }
    graphics.dispose()
    image
  }

  @throws[WriterException]
  private def getBitMatrix(data: String, width: Int, height: Int, hintsMap: java.util.Map[EncodeHintType, _]) = {
    val bitMatrix = qrCodeWriter.encode(data, BarcodeFormat.QR_CODE, width, height, hintsMap)
    bitMatrix
  }

  @throws[WriterException]
  private def getDefaultBitMatrix(data: String, hintsMap: java.util.Map[EncodeHintType, _]) = {
    val defaultBitMatrix = qrCodeWriter.encode(data, BarcodeFormat.QR_CODE, 0, 0, hintsMap)
    defaultBitMatrix
  }

  private def getHintsMap(errorCorrectionLevel: String, qrMargin: Int): java.util.Map[EncodeHintType, Any] = {
    val hintsMap: java.util.Map[EncodeHintType, Any] = new util.HashMap[EncodeHintType, Any]()
    errorCorrectionLevel match {
      case "H" =>
        hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.H)
      case "Q" =>
        hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.Q)
      case "M" =>
        hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.M)
      case "L" =>
        hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.L)
      case _ =>
        logger.error("Unknown error correction")
    }
    hintsMap.put(EncodeHintType.MARGIN, qrMargin)
    hintsMap
  }

  //Sample = 2A42UH , Verdana, 11, 0.1, Grayscale
  @throws[IOException]
  @throws[FontFormatException]
  private def getTextImage(text: String, fontName: String, fontSize: Int, tracking: Double, colorModel: String) = {
    var image: BufferedImage = new BufferedImage(1, 1, getImageType(colorModel))
    //Font basicFont = new Font(fontName, Font.BOLD, fontSize);
    val fontFile: String = fontName + ".ttf"
    var basicFont: Font = null
    var inputStream: InputStream = null
    val classLoader: ClassLoader = this.getClass.getClassLoader
    try {
      inputStream = classLoader.getResourceAsStream(fontFile)
      logger.info("input stream value is not null for fontfile " + fontFile + " " + inputStream)
      basicFont = Font.createFont(Font.TRUETYPE_FONT, inputStream)
    } catch {
      case e: Exception =>
        logger.debug("Exception occurred during font creation " + e)
    }
    val attributes: java.util.Map[TextAttribute, Any] = new java.util.HashMap[TextAttribute, Any]
    attributes.put(TextAttribute.TRACKING, tracking)
    attributes.put(TextAttribute.WEIGHT, TextAttribute.WEIGHT_BOLD)
    attributes.put(TextAttribute.SIZE, fontSize)
    val font = basicFont.deriveFont(attributes)
    var graphics2d = image.createGraphics
    graphics2d.setFont(font)
    var fontmetrics = graphics2d.getFontMetrics
    val width = fontmetrics.stringWidth(text)
    val height = fontmetrics.getHeight
    graphics2d.dispose()
    image = new BufferedImage(width, height, getImageType(colorModel))
    graphics2d = image.createGraphics
    graphics2d.setRenderingHint(RenderingHints.KEY_ALPHA_INTERPOLATION, RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY)
    graphics2d.setRenderingHint(RenderingHints.KEY_COLOR_RENDERING, RenderingHints.VALUE_COLOR_RENDER_QUALITY)
    graphics2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_OFF)
    graphics2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF)
    graphics2d.setColor(Color.WHITE)
    graphics2d.fillRect(0, 0, image.getWidth, image.getHeight)
    graphics2d.setColor(Color.BLACK)
    graphics2d.setFont(font)
    fontmetrics = graphics2d.getFontMetrics
    graphics2d.drawString(text, 0, fontmetrics.getAscent)
    graphics2d.dispose()
    image
  }

  private def getImageType(colorModel: String) = if ("RGB".equalsIgnoreCase(colorModel)) BufferedImage.TYPE_INT_RGB
  else BufferedImage.TYPE_BYTE_GRAY

}
