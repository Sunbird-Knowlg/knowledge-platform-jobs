package org.sunbird.incredible.pojos.exceptions

class InvalidDateFormatException(msg: String) extends Exception(msg) {}


class ServerException(code: String, msg: String) extends Exception(msg) {}
