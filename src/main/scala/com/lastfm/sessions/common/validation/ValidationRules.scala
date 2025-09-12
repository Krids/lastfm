package com.lastfm.sessions.common.validation

import com.lastfm.sessions.domain.{ValidationResult, Valid, Invalid}
import com.lastfm.sessions.common.{Constants, ErrorMessages}
import scala.util.{Try, Success, Failure}
import java.time.Instant

/**
 * Composable validation rules engine for complex validations.
 * 
 * Provides a fluent API for building complex validation chains
 * with clear error reporting and composition capabilities.
 * 
 * Design Principles:
 * - Composable rules with AND/OR logic
 * - Fail-fast validation with detailed errors
 * - Reusable validation components
 * - Type-safe validation chains
 * - Clear error message propagation
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object ValidationRules {
  
  /**
   * Base trait for all validation rules.
   * 
   * @tparam T Type being validated
   */
  trait ValidationRule[T] {
    /**
     * Validates the given value.
     * 
     * @param value Value to validate
     * @return Validation result with cleaned value or error
     */
    def validate(value: T): ValidationResult[T]
    
    /**
     * Error message for this validation rule.
     */
    def errorMessage: String
    
    /**
     * Compose rules with AND logic - both rules must pass.
     * 
     * @param other Rule to combine with
     * @return Combined rule that requires both to pass
     */
    def and(other: ValidationRule[T]): ValidationRule[T] = {
      new CompositeRule[T](this, other, AndLogic)
    }
    
    /**
     * Compose rules with OR logic - either rule can pass.
     * 
     * @param other Rule to combine with  
     * @return Combined rule that requires either to pass
     */
    def or(other: ValidationRule[T]): ValidationRule[T] = {
      new CompositeRule[T](this, other, OrLogic)
    }
    
    /**
     * Transform the validated value after successful validation.
     * 
     * @param f Transformation function
     * @tparam U Target type
     * @return Rule that validates and transforms
     */
    def map[U](f: T => U): ValidationRule[U] = {
      new TransformedRule[T, U](this, f)
    }
    
    /**
     * Add context information to validation errors.
     * 
     * @param context Context information
     * @return Rule with contextual error messages
     */
    def withContext(context: Map[String, Any]): ValidationRule[T] = {
      new ContextualRule[T](this, context)
    }
  }
  
  // Composition logic types
  sealed trait CompositionLogic
  case object AndLogic extends CompositionLogic
  case object OrLogic extends CompositionLogic
  
  /**
   * Composite rule for combining validations with AND/OR logic.
   */
  class CompositeRule[T](
    rule1: ValidationRule[T],
    rule2: ValidationRule[T],
    logic: CompositionLogic
  ) extends ValidationRule[T] {
    
    def validate(value: T): ValidationResult[T] = {
      logic match {
        case AndLogic =>
          rule1.validate(value) match {
            case Valid(v) => rule2.validate(v)
            case invalid => invalid
          }
        case OrLogic =>
          rule1.validate(value) match {
            case valid: Valid[T] => valid
            case Invalid(_) => rule2.validate(value)
          }
      }
    }
    
    def errorMessage: String = {
      val logicStr = logic match {
        case AndLogic => " AND "
        case OrLogic => " OR "
      }
      s"(${rule1.errorMessage}${logicStr}${rule2.errorMessage})"
    }
  }
  
  /**
   * Rule that transforms the value after successful validation.
   */
  class TransformedRule[T, U](
    baseRule: ValidationRule[T], 
    transform: T => U
  ) extends ValidationRule[U] {
    
    def validate(value: U): ValidationResult[U] = {
      // Note: This is a simplified implementation
      // In practice, you'd need reverse transformation or different approach
      Valid(value)
    }
    
    def errorMessage: String = baseRule.errorMessage
  }
  
  /**
   * Rule that adds context to error messages.
   */
  class ContextualRule[T](
    baseRule: ValidationRule[T],
    context: Map[String, Any]
  ) extends ValidationRule[T] {
    
    def validate(value: T): ValidationResult[T] = {
      baseRule.validate(value) match {
        case Valid(v) => Valid(v)
        case Invalid(msg) => Invalid(ErrorMessages.withContext(msg, context))
      }
    }
    
    def errorMessage: String = baseRule.errorMessage
  }
  
  /**
   * Common validation rules library.
   */
  object Rules {
    
    /**
     * Rule that validates value is not null.
     */
    def notNull[T]: ValidationRule[T] = new ValidationRule[T] {
      def validate(value: T): ValidationResult[T] = {
        if (value != null) Valid(value)
        else Invalid(ErrorMessages.Validation.NULL_VALUE)
      }
      def errorMessage = ErrorMessages.Validation.NULL_VALUE
    }
    
    /**
     * Rule that validates string is not empty.
     */
    def notEmpty(fieldName: String): ValidationRule[String] = new ValidationRule[String] {
      def validate(value: String): ValidationResult[String] = {
        if (value != null && value.trim.nonEmpty) Valid(value.trim)
        else Invalid(ErrorMessages.Validation.emptyField(fieldName))
      }
      def errorMessage = ErrorMessages.Validation.emptyField(fieldName)
    }
    
    /**
     * Rule that validates string matches a pattern.
     */
    def matchesPattern(pattern: String, description: String): ValidationRule[String] = new ValidationRule[String] {
      def validate(value: String): ValidationResult[String] = {
        if (value != null && value.matches(pattern)) Valid(value)
        else Invalid(ErrorMessages.Validation.invalidFormat(description))
      }
      def errorMessage = ErrorMessages.Validation.invalidFormat(description)
    }
    
    /**
     * Rule that validates numeric value is within range.
     */
    def inRange(min: Double, max: Double, fieldName: String): ValidationRule[Double] = new ValidationRule[Double] {
      def validate(value: Double): ValidationResult[Double] = {
        if (value >= min && value <= max) Valid(value)
        else Invalid(ErrorMessages.Validation.outOfRange(fieldName, min, max))
      }
      def errorMessage = ErrorMessages.Validation.outOfRange(fieldName, min, max)
    }
    
    /**
     * Rule that validates string length is within limits.
     */
    def maxLength(max: Int, fieldName: String): ValidationRule[String] = new ValidationRule[String] {
      def validate(value: String): ValidationResult[String] = {
        if (value == null || value.length <= max) Valid(value)
        else Invalid(ErrorMessages.Validation.tooLong(fieldName, max))
      }
      def errorMessage = ErrorMessages.Validation.tooLong(fieldName, max)
    }
    
    /**
     * Rule that validates string has minimum length.
     */
    def minLength(min: Int, fieldName: String): ValidationRule[String] = new ValidationRule[String] {
      def validate(value: String): ValidationResult[String] = {
        if (value != null && value.length >= min) Valid(value)
        else Invalid(ErrorMessages.Validation.tooShort(fieldName, min))
      }
      def errorMessage = ErrorMessages.Validation.tooShort(fieldName, min)
    }
    
    /**
     * Rule with custom validation logic.
     */
    def custom[T](predicate: T => Boolean, error: String): ValidationRule[T] = new ValidationRule[T] {
      def validate(value: T): ValidationResult[T] = {
        if (predicate(value)) Valid(value)
        else Invalid(error)
      }
      def errorMessage = error
    }
    
    /**
     * Rule that always passes validation.
     */
    def alwaysValid[T]: ValidationRule[T] = new ValidationRule[T] {
      def validate(value: T): ValidationResult[T] = Valid(value)
      def errorMessage = "Always valid"
    }
    
    /**
     * Rule that validates collection size.
     */
    def collectionSize[T](min: Int, max: Int, fieldName: String): ValidationRule[Iterable[T]] = new ValidationRule[Iterable[T]] {
      def validate(value: Iterable[T]): ValidationResult[Iterable[T]] = {
        if (value == null) {
          Invalid(ErrorMessages.Validation.NULL_VALUE)
        } else {
          val size = value.size
          if (size < min) {
            Invalid(s"$fieldName must contain at least $min items, got $size")
          } else if (size > max) {
            Invalid(s"$fieldName cannot contain more than $max items, got $size")
          } else {
            Valid(value)
          }
        }
      }
      def errorMessage = s"$fieldName size must be between $min and $max"
    }
    
    /**
     * Rule that validates positive numbers.
     */
    def positive(fieldName: String): ValidationRule[Double] = new ValidationRule[Double] {
      def validate(value: Double): ValidationResult[Double] = {
        if (value > 0) Valid(value)
        else Invalid(s"$fieldName must be positive, got $value")
      }
      def errorMessage = s"$fieldName must be positive"
    }
    
    /**
     * Rule that validates non-negative numbers.
     */
    def nonNegative(fieldName: String): ValidationRule[Double] = new ValidationRule[Double] {
      def validate(value: Double): ValidationResult[Double] = {
        if (value >= 0) Valid(value)
        else Invalid(s"$fieldName cannot be negative, got $value")
      }
      def errorMessage = s"$fieldName cannot be negative"
    }
  }
  
  /**
   * Domain-specific validation rules for LastFM data.
   */
  object DomainRules {
    import Rules._
    
    /**
     * Validates LastFM user ID format.
     */
    val userIdValidation: ValidationRule[String] = 
      notEmpty("userId")
        .and(matchesPattern(Constants.DataPatterns.USER_ID_PATTERN, "user_XXXXXX format"))
        .and(maxLength(Constants.Limits.MAX_USER_ID_LENGTH, "userId"))
    
    /**
     * Validates track name format.
     */
    val trackNameValidation: ValidationRule[String] = 
      notEmpty("trackName")
        .and(maxLength(Constants.Limits.MAX_TRACK_NAME_LENGTH, "trackName"))
    
    /**
     * Validates artist name format.
     */
    val artistNameValidation: ValidationRule[String] =
      notEmpty("artistName")
        .and(maxLength(Constants.Limits.MAX_ARTIST_NAME_LENGTH, "artistName"))
    
    /**
     * Validates quality score percentage.
     */
    val qualityScoreValidation: ValidationRule[Double] = 
      inRange(0.0, 100.0, "qualityScore")
    
    /**
     * Validates session gap value.
     */
    val sessionGapValidation: ValidationRule[Int] = 
      custom[Int](_ > 0, "Session gap must be positive")
        .and(custom[Int](_ <= Constants.Limits.MAX_SESSION_DURATION_MINUTES, 
          s"Session gap cannot exceed ${Constants.Limits.MAX_SESSION_DURATION_MINUTES} minutes"))
    
    /**
     * Validates ISO 8601 timestamp format.
     */
    val timestampValidation: ValidationRule[String] = 
      notEmpty("timestamp")
        .and(custom[String](ts => {
          Try(Instant.parse(ts)) match {
            case Success(_) => true
            case Failure(_) => false
          }
        }, "Invalid ISO 8601 timestamp format"))
    
    /**
     * Validates MusicBrainz ID format.
     */
    val mbidValidation: ValidationRule[String] =
      custom[String](mbid => {
        mbid == null || mbid.isEmpty || mbid.matches(Constants.DataPatterns.MBID_UUID_PATTERN)
      }, "Invalid MusicBrainz ID format (must be UUID)")
    
    /**
     * Validates partition count.
     */
    val partitionCountValidation: ValidationRule[Int] =
      custom[Int](_ > 0, "Partition count must be positive")
        .and(custom[Int](_ <= Constants.Partitioning.MAX_PARTITIONS, 
          s"Partition count cannot exceed ${Constants.Partitioning.MAX_PARTITIONS}"))
    
    /**
     * Validates top session/track count parameters.
     */
    val topCountValidation: ValidationRule[Int] =
      custom[Int](_ > 0, "Top count must be positive")
        .and(custom[Int](_ <= 1000, "Top count cannot exceed 1000"))
    
    /**
     * Validates memory size values.
     */
    val memorySizeValidation: ValidationRule[Long] =
      custom[Long](_ >= 0, "Memory size cannot be negative")
        .and(custom[Long](_ <= Runtime.getRuntime.maxMemory(), "Memory size exceeds available memory"))
  }
  
  /**
   * Validation chain builder for complex scenarios.
   */
  class ValidationChain[T](rules: List[ValidationRule[T]]) {
    
    /**
     * Validates value against all rules in sequence.
     */
    def validate(value: T): ValidationResult[T] = {
      rules.foldLeft[ValidationResult[T]](Valid(value)) { (result, rule) =>
        result match {
          case Valid(v) => rule.validate(v)
          case invalid => invalid
        }
      }
    }
    
    /**
     * Validates all values in a collection.
     */
    def validateAll(values: Seq[T]): Seq[ValidationResult[T]] = {
      values.map(validate)
    }
    
    /**
     * Validates value and returns detailed report.
     */
    def validateWithReport(value: T): ValidationReport[T] = {
      val results = rules.map(rule => (rule.errorMessage, rule.validate(value)))
      ValidationReport(value, results)
    }
    
    /**
     * Adds a rule to the chain.
     */
    def addRule(rule: ValidationRule[T]): ValidationChain[T] = {
      new ValidationChain(rules :+ rule)
    }
  }
  
  /**
   * Validation report containing detailed results.
   */
  case class ValidationReport[T](
    value: T,
    results: List[(String, ValidationResult[T])]
  ) {
    /**
     * Whether all validations passed.
     */
    def isValid: Boolean = results.forall(_._2.isInstanceOf[Valid[_]])
    
    /**
     * List of all error messages.
     */
    def errors: List[String] = results.collect {
      case (_, Invalid(msg)) => msg
    }
    
    /**
     * First error message, if any.
     */
    def firstError: Option[String] = errors.headOption
    
    /**
     * Summary of validation results.
     */
    def summary: String = {
      val totalRules = results.size
      val passedRules = results.count(_._2.isInstanceOf[Valid[_]])
      s"$passedRules/$totalRules validations passed"
    }
  }
  
  /**
   * Builder for creating validation chains fluently.
   */
  object ValidationChainBuilder {
    
    /**
     * Starts building a validation chain.
     */
    def forType[T]: ValidationChainBuilder[T] = new ValidationChainBuilder[T](List.empty)
    
    class ValidationChainBuilder[T](rules: List[ValidationRule[T]]) {
      
      /**
       * Adds a rule to the chain.
       */
      def withRule(rule: ValidationRule[T]): ValidationChainBuilder[T] = {
        new ValidationChainBuilder(rules :+ rule)
      }
      
      /**
       * Builds the validation chain.
       */
      def build: ValidationChain[T] = new ValidationChain(rules)
    }
  }
  
  /**
   * Utility methods for working with validation results.
   */
  object ValidationUtils {
    
    /**
     * Combines multiple validation results into a single result.
     */
    def combineResults[T](results: List[ValidationResult[T]]): ValidationResult[List[T]] = {
      val errors = results.collect { case Invalid(msg) => msg }
      if (errors.nonEmpty) {
        Invalid(errors.mkString("; "))
      } else {
        val values = results.collect { case Valid(value) => value }
        Valid(values)
      }
    }
    
    /**
     * Maps validation result to a different type.
     */
    def mapResult[T, U](result: ValidationResult[T])(f: T => U): ValidationResult[U] = {
      result match {
        case Valid(value) => Valid(f(value))
        case Invalid(msg) => Invalid(msg)
      }
    }
    
    /**
     * Flat maps validation result.
     */
    def flatMapResult[T, U](result: ValidationResult[T])(f: T => ValidationResult[U]): ValidationResult[U] = {
      result match {
        case Valid(value) => f(value)
        case Invalid(msg) => Invalid(msg)
      }
    }
    
    /**
     * Converts validation result to Try.
     */
    def toTry[T](result: ValidationResult[T]): Try[T] = {
      result match {
        case Valid(value) => Success(value)
        case Invalid(msg) => Failure(new IllegalArgumentException(msg))
      }
    }
    
    /**
     * Converts Try to validation result.
     */
    def fromTry[T](tryValue: Try[T]): ValidationResult[T] = {
      tryValue match {
        case Success(value) => Valid(value)
        case Failure(exception) => Invalid(exception.getMessage)
      }
    }
  }
}