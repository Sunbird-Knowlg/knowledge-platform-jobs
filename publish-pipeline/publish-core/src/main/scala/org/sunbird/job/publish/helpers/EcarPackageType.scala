package org.sunbird.job.publish.helpers

object EcarPackageType extends Enumeration {

  val FULL: String = Value("FULL").toString
  val SPINE: String = Value("SPINE").toString
  val OPTIMIZED: String = Value("OPTIMIZED").toString
  val ONLINE: String = Value("ONLINE").toString
}
