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
  * Checks required fields, excluded fields, and field types.
  * @param fieldConfig Field configuration to validate against
  */
class FieldValidator(fieldConfig: FieldConfiguration) {
  private val logger = LoggerFactory.getLogger(classOf[FieldValidator])

  /** Validate enriched event against field configuration.
    * Checks: required fields present, excluded fields absent, field types valid.
    * @param objectType Object type (Content, Collection, Question, QuestionSet)
    * @param eventData Event data to validate
    * @return ValidationResult with isValid flag and list of errors
    */
  def validateEnrichedEvent(objectType: String, eventData: Map[String, Any]): ValidationResult = {
    val errors = scala.collection.mutable.ArrayBuffer[ValidationError]()

    // Check required fields
    val requiredFields = fieldConfig.getRequiredFieldsFor(objectType)
    requiredFields.foreach { reqField =>
      if (!eventData.contains(reqField.name)) {
        errors += ValidationError(reqField.name, s"Required field missing: ${reqField.name}")
      } else {
        val value = eventData(reqField.name)
        if (value == null) {
          errors += ValidationError(reqField.name, s"Required field is null: ${reqField.name}")
        }
      }
    }

    // Check for excluded fields
    val excludedFields = fieldConfig.getExcludedFieldsFor(objectType)
    excludedFields.foreach { excludedField =>
      if (eventData.contains(excludedField)) {
        errors += ValidationError(excludedField, s"Excluded field present: $excludedField")
      }
    }

    // Check field types
    eventData.foreach { case (fieldName, value) =>
      val typeOpt = fieldConfig.getFieldType(objectType, fieldName)
      typeOpt.foreach { expectedType =>
        val typeError = validateFieldType(fieldName, value, expectedType)
        typeError.foreach(errors += _)
      }
    }

    ValidationResult(
      isValid = errors.isEmpty,
      errors = errors.toSeq
    )
  }

  private def validateFieldType(fieldName: String, value: Any, expectedType: String): Option[ValidationError] = {
    if (value == null) {
      return None
    }

    val typeMatch = expectedType.toLowerCase() match {
      case "string" => value.isInstanceOf[String]
      case "integer" | "int" => value.isInstanceOf[Int] || value.isInstanceOf[Long] || value.isInstanceOf[Number]
      case "number" | "float" | "double" => value.isInstanceOf[Number]
      case "boolean" | "bool" => value.isInstanceOf[Boolean]
      case "array" => value.isInstanceOf[Seq[_]] || value.isInstanceOf[java.util.List[_]]
      case "object" | "map" => value.isInstanceOf[Map[_, _]] || value.isInstanceOf[java.util.Map[_, _]]
      case _ =>
        logger.warn(s"Unknown field type: $expectedType for field $fieldName")
        true
    }

    if (!typeMatch) {
      Some(ValidationError(
        fieldName,
        s"Type mismatch. Expected: $expectedType, Got: ${value.getClass.getSimpleName}"
      ))
    } else {
      None
    }
  }

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
