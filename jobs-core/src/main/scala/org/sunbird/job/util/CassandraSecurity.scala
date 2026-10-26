package org.sunbird.job.util

import com.datastax.driver.core.{Cluster, RemoteEndpointAwareJdkSSLOptions}
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.BaseJobConfig

import java.io.FileInputStream
import java.security.KeyStore
import java.security.cert.CertificateFactory
import javax.net.ssl.{SSLContext, TrustManagerFactory}
import scala.collection.JavaConverters._

/**
 * Adds credentials and TLS to a Cassandra [[Cluster.Builder]]. Both are off by default, so
 * existing deployments are unaffected.
 *
 * Config keys (read from the job config): `cassandra.auth.username`, `cassandra.auth.password`,
 * `cassandra.ssl.enabled`, `cassandra.ssl.rootcert`.
 */
object CassandraSecurity {

  def configure(builder: Cluster.Builder, config: BaseJobConfig): Cluster.Builder =
    configure(builder,
      config.getString("cassandra.auth.username", ""),
      config.getString("cassandra.auth.password", ""),
      config.getBoolean("cassandra.ssl.enabled", false),
      config.getString("cassandra.ssl.rootcert", ""))

  /**
   * Credentials are added only when username and password are both non-empty. TLS trusts only the
   * CA(s) in `rootCertPath` (PEM, may hold several certificates).
   * ponytail: chain and dates are verified, hostname is not (driver 3.x default). Add endpoint
   * identification if a hostname-pinned check is required.
   */
  def configure(builder: Cluster.Builder, username: String, password: String,
                sslEnabled: Boolean, rootCertPath: String): Cluster.Builder = {
    if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password))
      builder.withCredentials(username, password)
    if (sslEnabled)
      builder.withSSL(RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(buildSslContext(rootCertPath)).build())
    builder
  }

  /** @throws IllegalStateException if the path is blank, unreadable or holds no certificate */
  def buildSslContext(rootCertPath: String): SSLContext = {
    if (StringUtils.isBlank(rootCertPath))
      throw new IllegalStateException("cassandra.ssl.rootcert must be set when cassandra.ssl.enabled is true")
    try {
      val in = new FileInputStream(rootCertPath)
      val certs = try CertificateFactory.getInstance("X.509").generateCertificates(in).asScala.toList finally in.close()
      if (certs.isEmpty) throw new IllegalStateException(s"No certificate found in $rootCertPath")
      val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
      trustStore.load(null, null)
      certs.zipWithIndex.foreach { case (cert, i) => trustStore.setCertificateEntry(s"cassandra-ca-$i", cert) }
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(trustStore)
      val context = SSLContext.getInstance("TLS")
      context.init(null, tmf.getTrustManagers, null)
      context
    } catch {
      case e: IllegalStateException => throw e
      case e: Exception =>
        throw new IllegalStateException(s"Failed to build Cassandra SSL context from $rootCertPath: ${e.getMessage}", e)
    }
  }
}
