package org.sunbird.job.qrimagegenerator.util

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.zxing.client.j2se.BufferedImageLuminanceSource
import com.google.zxing.common.{BitMatrix, HybridBinarizer}
import com.google.zxing.qrcode.QRCodeWriter
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel
import com.google.zxing.{BarcodeFormat, EncodeHintType, NotFoundException, WriterException}
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.qrimagegenerator.domain.{ImageConfig, QRCodeImageGeneratorRequest}
import org.sunbird.job.qrimagegenerator.task.QRCodeImageGeneratorConfig
import org.sunbird.job.util._

import java.awt.font.TextAttribute
import java.awt.image.BufferedImage
import java.awt.{Color, Font, FontFormatException, Graphics2D, RenderingHints}
import java.io.{File, IOException, InputStream}
import java.util.UUID
import javax.imageio.ImageIO
import scala.collection.mutable.ListBuffer

class QRCodeImageGeneratorUtil(config: QRCodeImageGeneratorConfig, cassandraUtil: CassandraUtil, cloudStorageUtil: CloudStorageUtil) {
  private val qrCodeWriter = new QRCodeWriter()
  private val fontStore: java.util.Map[String, Font] = new java.util.HashMap[String, Font]()
  private val logger = LoggerFactory.getLogger(classOf[QRCodeImageGeneratorUtil])

  @throws[WriterException]
  @throws[IOException]
  @throws[NotFoundException]
  @throws[FontFormatException]
  def createQRImages(req: QRCodeImageGeneratorRequest, container: String, path: String, metrics: Metrics): ListBuffer[File] = {
    val fileList = ListBuffer[File]()
    val imageConfig: ImageConfig = req.imageConfig
    val dialCodes: List[Map[String, AnyRef]] = req.dialCodes

    dialCodes.foreach { dialcode =>
      val data = dialcode("data").asInstanceOf[String]
      val text = dialcode("text").asInstanceOf[String]
      val fileName = dialcode("id").asInstanceOf[String]
      logger.info("QRCodeImageGeneratorUtil:createQRImages: data - " + data+" text - "+text+" fileName - "+fileName)
      var qrImage = generateBaseImage(data, imageConfig.errorCorrectionLevel, imageConfig.pixelsPerBlock, imageConfig.qrCodeMargin, imageConfig.colourModel)
      if (null != text || !text.isBlank) {
        val textImage = getTextImage(text, imageConfig.textFontName, imageConfig.textFontSize, imageConfig.textCharacterSpacing, imageConfig.colourModel)
        qrImage = addTextToBaseImage(qrImage, textImage, imageConfig.colourModel, imageConfig.qrCodeMargin, imageConfig.pixelsPerBlock, imageConfig.qrCodeMarginBottom, imageConfig.imageMargin)
      }
      logger.info("QRCodeImageGeneratorUtil:createQRImages: qrImage - " + qrImage)
      if (imageConfig.imageBorderSize > 0) drawBorder(qrImage, imageConfig.imageBorderSize, imageConfig.imageMargin)
      val finalImageFile = new File(req.tempFilePath + File.separator + fileName + "." + imageConfig.imageFormat)
      logger.info("QRCodeImageGeneratorUtil:createQRImages: creating file - " + finalImageFile.getAbsolutePath)
      finalImageFile.createNewFile
      ImageIO.write(qrImage, imageConfig.imageFormat, finalImageFile)
      fileList += finalImageFile
      try {
        logger.info("QRCodeImageGeneratorUtil:createQRImages: path before - " + path)
        val imageDownloadUrl = cloudStorageUtil.uploadFile(path, finalImageFile, Some(false), container = container)
        logger.info("QRCodeImageGeneratorUtil:createQRImages: imageDownloadUrl - " + imageDownloadUrl.mkString(","))
        logger.info("QRCodeImageGeneratorUtil:createQRImages: imageDownloadUrl(1) - " + imageDownloadUrl(1))

        var newDownloadUrl = imageDownloadUrl(1).replaceAll("bmzbbujw9kal.compat.objectstorage.ap-mumbai-1.oraclecloud.com", "files.odev.oci.diksha.gov.in")
        logger.info("QRCodeImageGeneratorService:processMessage: newDownloadUrl before - " + newDownloadUrl)
        updateCassandra(config.cassandraDialCodeImageTable, 2, newDownloadUrl, "filename", fileName, metrics)
      } catch {
        case e: Exception =>
          metrics.incCounter(config.dbFailureEventCount)
          e.printStackTrace()
          logger.info("QRCodeImageGeneratorUtil: Failure while uploading download URL Query:: " + e.getMessage)
      }
    }
    fileList
  }

  def updateCassandra(table: String, status: Int, downloadURL: String, whereClauseKey: String, whereClauseValue: String, metrics: Metrics): Unit = {
    val value = if (whereClauseKey.equalsIgnoreCase("processid")) UUID.fromString(whereClauseValue) else whereClauseValue
    val updateQuery: String = QueryBuilder.update(config.cassandraKeyspace, table)
      .`with`(QueryBuilder.set("status", status))
      .and(QueryBuilder.set("url", downloadURL))
      .where(QueryBuilder.eq(whereClauseKey, value)).toString
    cassandraUtil.upsert(updateQuery)
    metrics.incCounter(config.dbHitEventCount)
  }

  @throws[NotFoundException]
  private def addTextToBaseImage(qrImage: BufferedImage, textImage: BufferedImage, colorModel: String, qrMargin: Int, pixelsPerBlock: Int, qrMarginBottom: Int, imageMargin: Int) = {
    val qrSource = new BufferedImageLuminanceSource(qrImage)
    val qrBinarizer = new HybridBinarizer(qrSource)
    var qrBits = qrBinarizer.getBlackMatrix
    val textSource = new BufferedImageLuminanceSource(textImage)
    val textBinarizer = new HybridBinarizer(textSource)
    var textBits = textBinarizer.getBlackMatrix
    if (qrBits.getWidth > textBits.getWidth) {
      val tempTextMatrix = new BitMatrix(qrBits.getWidth, textBits.getHeight)
      copyMatrixDataToBiggerMatrix(textBits, tempTextMatrix)
      textBits = tempTextMatrix
    }
    else if (qrBits.getWidth < textBits.getWidth) {
      val tempQrMatrix = new BitMatrix(textBits.getWidth, qrBits.getHeight)
      copyMatrixDataToBiggerMatrix(qrBits, tempQrMatrix)
      qrBits = tempQrMatrix
    }
    val mergedMatrix = mergeMatricesOfSameWidth(qrBits, textBits, qrMargin, pixelsPerBlock, qrMarginBottom, imageMargin)
    getImage(mergedMatrix, colorModel)
  }

  @throws[WriterException]
  private def generateBaseImage(data: String, errorCorrectionLevel: String, pixelsPerBlock: Int, qrMargin: Int, colorModel: String) = {
    val hintsMap = getHintsMap(errorCorrectionLevel, qrMargin)
    val defaultBitMatrix = getDefaultBitMatrix(data, hintsMap)
    logger.info(s"defaultBitMatrix: width= ${defaultBitMatrix.getWidth}, height= ${defaultBitMatrix.getHeight}")
    val largeBitMatrix = getBitMatrix(data, defaultBitMatrix.getWidth * pixelsPerBlock, defaultBitMatrix.getHeight * pixelsPerBlock, hintsMap)
    logger.info(s"largeBitMatrix: width=  ${largeBitMatrix.getWidth}, height= ${largeBitMatrix.getHeight}")
    getImage(largeBitMatrix, colorModel)
  }

  //To remove extra spaces between text and qrcode, margin below qrcode is removed
  //Parameter, qrCodeMarginBottom, is introduced to add custom margin(in pixels) between qrcode and text
  //Parameter, imageMargin is introduced, to add custom margin(in pixels) outside the black border of the image
  private def mergeMatricesOfSameWidth(firstMatrix: BitMatrix, secondMatrix: BitMatrix, qrMargin: Int, pixelsPerBlock: Int, qrMarginBottom: Int, imageMargin: Int) = {
    val mergedWidth = firstMatrix.getWidth + (2 * imageMargin)
    val mergedHeight = firstMatrix.getHeight + secondMatrix.getHeight + (2 * imageMargin)
    val defaultBottomMargin = pixelsPerBlock * qrMargin
    val marginToBeRemoved = if (qrMarginBottom > defaultBottomMargin) 0
    else defaultBottomMargin - qrMarginBottom
    val mergedMatrix = new BitMatrix(mergedWidth, mergedHeight - marginToBeRemoved)
    for (x <- 0 until firstMatrix.getWidth) {
      for (y <- 0 until firstMatrix.getHeight) {
        if (firstMatrix.get(x, y)) mergedMatrix.set(x + imageMargin, y + imageMargin)
      }
    }

    for (x <- 0 until secondMatrix.getWidth) {
      for (y <- 0 until secondMatrix.getHeight) {
        if (secondMatrix.get(x, y)) mergedMatrix.set(x + imageMargin, y + firstMatrix.getHeight - marginToBeRemoved + imageMargin)
      }
    }
    mergedMatrix
  }

  private def copyMatrixDataToBiggerMatrix(fromMatrix: BitMatrix, toMatrix: BitMatrix): Unit = {
    val widthDiff = toMatrix.getWidth - fromMatrix.getWidth
    val leftMargin = widthDiff / 2

    for (x <- 0 until fromMatrix.getWidth) {
      for (y <- 0 until fromMatrix.getHeight) {
        if (fromMatrix.get(x, y)) toMatrix.set(x + leftMargin, y)
      }
    }
  }

  private def drawBorder(image: BufferedImage, borderSize: Int, imageMargin: Int): Unit = {
    image.createGraphics
    val graphics = image.getGraphics.asInstanceOf[Graphics2D]
    graphics.setColor(Color.BLACK)
    for (i <- 0 until borderSize) {
      graphics.drawRect(i + imageMargin, i + imageMargin, image.getWidth - 1 - (2 * i) - (2 * imageMargin), image.getHeight - 1 - (2 * i) - (2 * imageMargin))
    }
    graphics.dispose()
  }

  private def getImage(bitMatrix: BitMatrix, colorModel: String) = {
    val imageWidth = bitMatrix.getWidth()
    val imageHeight = bitMatrix.getHeight()
    val image = new BufferedImage(imageWidth, imageHeight, getImageType(colorModel))
    image.createGraphics()
    val graphics = image.getGraphics.asInstanceOf[Graphics2D]
    graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF)
    graphics.setColor(Color.WHITE)
    graphics.fillRect(0, 0, imageWidth, imageHeight)
    graphics.setColor(Color.BLACK)
    logger.info(s"imageWidth: $imageWidth, imageHeight: $imageHeight")
    for (i <- 0 until imageWidth) {
      for (j <- 0 until imageHeight) {
        if (bitMatrix.get(i, j)) {
          graphics.fillRect(i, j, 1, 1)
        }
      }
    }
    graphics.dispose()
    image
  }

  @throws[WriterException]
  private def getBitMatrix(data: String, width: Int, height: Int, hintsMap: java.util.Map[_, _]) = {
    qrCodeWriter.encode(data, BarcodeFormat.QR_CODE, width, height, hintsMap.asInstanceOf[java.util.Map[EncodeHintType, _]])
  }

  @throws[WriterException]
  private def getDefaultBitMatrix(data: String, hintsMap: java.util.Map[_, _]) = {
    qrCodeWriter.encode(data, BarcodeFormat.QR_CODE, 0, 0, hintsMap.asInstanceOf[java.util.Map[EncodeHintType, _]])
  }

  private def getHintsMap(errorCorrectionLevel: String, qrMargin: Int) = {
    val hintsMap = new java.util.HashMap[EncodeHintType, AnyRef]()
    errorCorrectionLevel match {
      case "H" => hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.H)
      case "Q" => hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.Q)
      case "M" => hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.M)
      case "L" => hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.L)
    }
    hintsMap.put(EncodeHintType.MARGIN, qrMargin.asInstanceOf[AnyRef])
    hintsMap
  }

  //Sample = 2A42UH , Verdana, 11, 0.1, Grayscale
  @throws[IOException]
  @throws[FontFormatException]
  private def getTextImage(text: String, fontName: String, fontSize: Int, tracking: Double, colorModel: String) = {
    var image = new BufferedImage(1, 1, getImageType(colorModel))
    val basicFont = getFontFromStore(fontName)
    val attributes: java.util.Map[TextAttribute, Any] = new java.util.HashMap[TextAttribute, Any]
    attributes.put(TextAttribute.TRACKING, tracking)
    attributes.put(TextAttribute.WEIGHT, TextAttribute.WEIGHT_BOLD)
    attributes.put(TextAttribute.SIZE, fontSize)
    logger.info("attributes: " + attributes)
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

  private def getImageType(colorModel: String) = if (colorModel.equalsIgnoreCase("RGB")) BufferedImage.TYPE_INT_RGB
  else BufferedImage.TYPE_BYTE_GRAY

  @throws[IOException]
  @throws[FontFormatException]
  //load the packaged font file from the root dir
  private def loadFontStore(fontName: String): Font = {
    val fontFile = fontName + ".ttf"
    logger.info("fontFile: " + fontFile)
    var basicFont: Font = null
    var fontStream: InputStream = null
    val classLoader: ClassLoader = this.getClass.getClassLoader
    try {
      fontStream = classLoader.getResourceAsStream(fontFile)
      logger.info("input stream value is not null for fontfile " + fontFile + " " + fontStream)
      basicFont = Font.createFont(Font.TRUETYPE_FONT, fontStream)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.debug("Exception occurred during font creation " + e.getMessage)
    }
    fontStore.put(fontName, basicFont)
    basicFont
  }

  @throws[IOException]
  @throws[FontFormatException]
  private def getFontFromStore(fontName: String): Font = {
    fontStore.getOrDefault(fontName, loadFontStore(fontName))
  }
}

