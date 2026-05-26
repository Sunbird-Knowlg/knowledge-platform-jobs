package org.sunbird.job.publish.helpers

import org.slf4j.LoggerFactory
import scala.util.{Success, Failure, Try}

/** Error from field validation.
  * @param fieldName Name of field with validation error
  * @param reason Reason for validation failure
  */
case class ValidationError(fieldName: String, reason: String)

/** Result of field validation.
  * @param isValid Whether validation passed
  * @param errors List of validation errors found
  */
case class ValidationResult(
    isValid: Boolean,
    errors: Seq[ValidationError]
)

/** Validates enriched metadata events against field configuration.
  * Checks field presence in events.
  * @param fieldConfig Field configuration to validate against
  */
class FieldValidator(fieldConfig: FieldConfiguration) {
  private val logger = LoggerFactory.getLogger(classOf[FieldValidator])

  def validateFieldPresence(objectType: String, fields: Seq[String], eventData: Map[String, Any]): ValidationResult = {
    val errors = scala.collection.mutable.ArrayBuffer[ValidationError]()

    fields.foreach { fieldName =>
      if (!eventData.contains(fieldName)) {
        errors += ValidationError(fieldName, s"Field not found in event data: $fieldName")
      }
    }

    ValidationResult(
      isValid = errors.isEmpty,
      errors = errors.toSeq
    )
  }

  def throwIfInvalid(result: ValidationResult, objectType: String): Unit = {
    if (!result.isValid) {
      val errorSummary = result.errors.map(e => s"${e.fieldName}: ${e.reason}").mkString("; ")
      val message = s"Field validation failed for $objectType: $errorSummary"
      logger.error(message)
      throw new IllegalStateException(message)
    }
  }

  def logValidationResult(result: ValidationResult, objectType: String): Unit = {
    if (result.isValid) {
      logger.debug(s"Field validation passed for $objectType")
    } else {
      logger.warn(s"Field validation failed for $objectType with ${result.errors.length} errors:")
      result.errors.foreach(err => logger.warn(s"  - ${err.fieldName}: ${err.reason}"))
    }
  }

  def getSummary(result: ValidationResult): String = {
    if (result.isValid) {
      "Validation passed"
    } else {
      s"Validation failed with ${result.errors.length} errors: ${result.errors.map(_.fieldName).mkString(", ")}"
    }
  }
}
