package org.sunbird.incredible.processor.signature


case class CustomException(msg: String) extends Exception(msg) {}

@SerialVersionUID(-6315798195661762882L)
class SignatureException extends Exception {

  @SerialVersionUID(6174717850058203376L)
  class CreationException(msg: String)
    extends CustomException("Unable to create signature: " + msg)

  @SerialVersionUID(4996784337180620650L)
  class VerificationException(message: String)
    extends CustomException("Unable to verify signature " + message)

  @SerialVersionUID(5384120386096139083L)
  class UnreachableException(message: String)
    extends CustomException("Unable to reach service: " + message)

  @SerialVersionUID(8311355815972497247L)
  class KeyNotFoundException(message: String)
    extends CustomException("Unable to get key: " + message)

}
