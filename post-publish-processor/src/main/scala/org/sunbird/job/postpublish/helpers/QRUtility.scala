package org.sunbird.job.postpublish.helpers

import java.util

import org.ekstep.qrimage.generator.QRImageGenerator
import org.ekstep.qrimage.request.QRImageRequest
import org.slf4j.LoggerFactory
import org.sunbird.job.functions.ShallowCopyPublishFunction
import org.sunbird.job.models.ExtDataConfig
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil}

trait QRUtility {
    val qrRequest = new QRImageRequest("/tmp")
    private[this] val logger = LoggerFactory.getLogger(classOf[ShallowCopyPublishFunction])

    def generateQRImage(channel: String, dialcode: String)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil, cloudStorageUtil: CloudStorageUtil): Unit = {
        getQRImageUrl(channel, dialcode) match {
            case Some(url: String) => createQRImageRecord(dialcode, url, channel)
            case _ => logger.error("Error while generating QRImage Url")
        }
    }

    def getQRImageUrl(channel: String, dialcode: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
        val dialUrl = "https://dev.sunbirded.org/dial/" + dialcode
        qrRequest.setFileName("0_" + dialcode)
        qrRequest.setText(dialcode)
        qrRequest.setData(util.Arrays.asList(dialUrl))
        val file = QRImageGenerator.generateQRImage(qrRequest)
        try {
            val urls = cloudStorageUtil.uploadFile(channel, file, Some(false))
            if (urls.isEmpty) None else Some(urls(1))
        } catch {
            case e: Exception => if (null != file && file.exists) file.delete
                None
        }
    }

    def createQRImageRecord(dialcode: String, imageUrl: String, channel: String)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil): Unit = {
        val fileName = "0_" + dialcode
        val query =
            s"""
               | insert into ${extConfig.keyspace}.${extConfig.table} (filename, channel, dialcode, publisher, status, url, created_on, config)
               |            values ('$fileName', ' $channel', '$dialcode', null, 2, '$imageUrl',toTimestamp(now()), null)
             """.stripMargin
        cassandraUtil.session.execute(query)
    }

    def validateAndGenerateQR(dialcode: String, identifier: String,  channel: String)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil, cloudStorageUtil: CloudStorageUtil): Unit = {
        getQRImageRecord(dialcode) match {
            case Some(url: String) => logger.info(s"Event Skipped. Target Object $identifier already has DIAL Code and its QR Image.| DIAL Code : $dialcode")
            case _ => generateQRImage(channel , dialcode)
        }
    }

    def getQRImageRecord(dialcode: String)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil): Option[String] = {
        if (dialcode.isBlank) throw new Exception("Invalid dialcode to read")
        val fileName = s"0_$dialcode"
        val query = s"select url from ${extConfig.keyspace}.${extConfig.table} where filename = '$fileName';"
        try {
            val resultSet = cassandraUtil.session.execute(query)
            if (resultSet == null) None else Some(resultSet.one().getString("url"))
        } catch {
            case e: Exception => logger.error("There was an issue while fetching the qr image url from table")
                throw new Exception("There was an issue while fetching the qr image url from table")
        }
    }
}
