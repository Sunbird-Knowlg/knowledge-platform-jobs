package org.sunbird.incredible.pojos.exceptions

case class InvalidDateFormatException(msg: String) extends Exception(msg) {}


case class ServerException(code: String, msg: String) extends Exception(msg) {}
