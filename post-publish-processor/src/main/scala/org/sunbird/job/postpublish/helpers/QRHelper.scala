package org.sunbird.job.postpublish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.functions.ShallowCopyPublishFunction
import org.sunbird.job.models.ExtDataConfig
import org.sunbird.job.util.CassandraUtil

trait QRHelper {
    private[this] val logger = LoggerFactory.getLogger(classOf[ShallowCopyPublishFunction])

    def validateQR(dialcode: String)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil): Boolean = {
        getQRImageRecord(dialcode) match {
            case Some(url: String) => true
            case _ => false
        }
    }

    def getQRImageRecord(dialcode: String)(implicit extConfig: ExtDataConfig, cassandraUtil: CassandraUtil): Option[String] = {
        if (dialcode.isEmpty) throw new Exception("Invalid dialcode to read")
        val fileName = s"0_$dialcode"
        val query = s"select url from ${extConfig.keyspace}.${extConfig.table} where filename = '$fileName';"
        try {
            val resultSet = cassandraUtil.session.execute(query)
            val result = resultSet.one()
            if (result == null) None else Some(result.getString("url"))
        } catch {
            case e: Exception => logger.error("There was an issue while fetching the qr image url from table")
                throw new Exception("There was an issue while fetching the qr image url from table")
        }
    }
}
