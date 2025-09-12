package com.lastfm.sessions.common.traits

import com.lastfm.sessions.common.{Constants, ErrorMessages}
import com.lastfm.sessions.domain.{ValidationResult, Valid, Invalid}
import java.time.Instant
import java.util.UUID
import scala.util.{Try, Success, Failure}

/**
 * Trait providing common data validation patterns.
 * 
 * Encapsulates reusable validation logic for LastFM data processing,
 * ensuring consistent validation rules across the application.
 * 
 * Design Principles:
 * - Fail-fast validation with clear error messages
 * - Reusable validation patterns
 * - Domain-specific business rules
 * - Type-safe validation results
 * - Consistent error reporting
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait DataValidator {
  
  /**
   * Validates LastFM user ID format.
   * 
   * Expected format: user_XXXXXX (where X is a digit)
   * 
   * @param userId User ID to validate
   * @return Validation result with cleaned user ID or error
   */
  def validateUserId(userId: String): ValidationResult[String] = {
    if (userId == null) {
      Invalid(ErrorMessages.Validation.NULL_VALUE)
    } else if (userId.trim.isEmpty) {
      Invalid(ErrorMessages.Validation.emptyField("userId"))
    } else {
      val trimmedUserId = userId.trim
      if (!trimmedUserId.matches(Constants.DataPatterns.USER_ID_PATTERN)) {
        Invalid(ErrorMessages.Validation.invalidUserId(trimmedUserId))
      } else {
        Valid(trimmedUserId)
      }
    }
  }
  
  /**
   * Validates and parses ISO 8601 timestamp format.
   * 
   * @param timestamp Timestamp string to validate
   * @return Validation result with parsed Instant or error
   */
  def validateTimestamp(timestamp: String): ValidationResult[Instant] = {
    if (timestamp == null) {
      Invalid(ErrorMessages.Validation.NULL_VALUE)
    } else if (timestamp.trim.isEmpty) {
      Invalid(ErrorMessages.Validation.emptyField("timestamp"))
    } else {
      Try(Instant.parse(timestamp.trim)) match {
        case Success(instant) => 
          // Additional validation for reasonable timestamp bounds
          val now = Instant.now()
          val year2000 = Instant.parse("2000-01-01T00:00:00Z")
          val future = now.plusSeconds(365 * 24 * 3600) // 1 year from now
          
          if (instant.isBefore(year2000)) {
            Invalid(s"Timestamp too old: $timestamp (before year 2000)")
          } else if (instant.isAfter(future)) {
            Invalid(s"Timestamp too far in future: $timestamp")
          } else {
            Valid(instant)
          }
        case Failure(_) => 
          Invalid(ErrorMessages.Validation.invalidTimestamp(timestamp))
      }
    }
  }
  
  /**
   * Validates MusicBrainz ID (MBID) format.
   * 
   * @param mbid MBID to validate (can be null/empty for optional fields)
   * @return Validation result with cleaned MBID or error
   */
  def validateMBID(mbid: String): ValidationResult[Option[String]] = {
    if (mbid == null || mbid.trim.isEmpty) {
      Valid(None) // MBID is optional
    } else {
      val trimmedMbid = mbid.trim
      if (trimmedMbid.matches(Constants.DataPatterns.MBID_UUID_PATTERN)) {
        Valid(Some(trimmedMbid))
      } else {
        Invalid(ErrorMessages.Validation.invalidMBID(trimmedMbid))
      }
    }
  }
  
  /**
   * Validates and cleans track name.
   * 
   * @param name Track name to validate
   * @param defaultValue Default value for empty names
   * @return Cleaned track name (never null/empty)
   */
  def validateTrackName(name: String, defaultValue: String = "Unknown Track"): String = {
    if (name == null || name.trim.isEmpty) {
      defaultValue
    } else {
      cleanTextValue(name, Constants.Limits.MAX_TRACK_NAME_LENGTH)
    }
  }
  
  /**
   * Validates and cleans artist name.
   * 
   * @param name Artist name to validate
   * @param defaultValue Default value for empty names
   * @return Cleaned artist name (never null/empty)
   */
  def validateArtistName(name: String, defaultValue: String = "Unknown Artist"): String = {
    if (name == null || name.trim.isEmpty) {
      defaultValue
    } else {
      cleanTextValue(name, Constants.Limits.MAX_ARTIST_NAME_LENGTH)
    }
  }
  
  /**
   * Generates deterministic track key for identification.
   * 
   * Priority: trackId (MBID) > composite "artist — track" key
   * 
   * @param trackId Optional MusicBrainz track ID
   * @param artist Artist name
   * @param track Track name
   * @return Unique track identifier
   */
  def generateTrackKey(trackId: Option[String], artist: String, track: String): String = {
    trackId.filter(_.nonEmpty).getOrElse {
      s"$artist${Constants.DataPatterns.TRACK_KEY_SEPARATOR}$track"
    }
  }
  
  /**
   * Validates session gap value.
   * 
   * @param gapMinutes Session gap in minutes
   * @return Validation result
   */
  def validateSessionGap(gapMinutes: Int): ValidationResult[Int] = {
    if (gapMinutes <= 0) {
      Invalid(ErrorMessages.Session.invalidSessionGap(gapMinutes))
    } else if (gapMinutes > Constants.Limits.MAX_SESSION_DURATION_MINUTES) {
      Invalid(ErrorMessages.Session.invalidSessionGap(gapMinutes))
    } else {
      Valid(gapMinutes)
    }
  }
  
  /**
   * Validates quality score percentage.
   * 
   * @param score Quality score to validate (0-100)
   * @return Validation result
   */
  def validateQualityScore(score: Double): ValidationResult[Double] = {
    if (score < 0.0 || score > 100.0) {
      Invalid(ErrorMessages.Validation.outOfRange("qualityScore", 0.0, 100.0))
    } else {
      Valid(score)
    }
  }
  
  /**
   * Validates partition count.
   * 
   * @param partitions Number of partitions
   * @return Validation result
   */
  def validatePartitionCount(partitions: Int): ValidationResult[Int] = {
    if (partitions <= 0) {
      Invalid("Partition count must be positive")
    } else if (partitions > Constants.Partitioning.MAX_PARTITIONS) {
      Invalid(s"Partition count $partitions exceeds maximum ${Constants.Partitioning.MAX_PARTITIONS}")
    } else {
      Valid(partitions)
    }
  }
  
  /**
   * Validates file path format and accessibility.
   * 
   * @param path File path to validate
   * @param mustExist Whether the path must exist
   * @return Validation result
   */
  def validateFilePath(path: String, mustExist: Boolean = false): ValidationResult[String] = {
    if (path == null) {
      Invalid(ErrorMessages.Validation.NULL_VALUE)
    } else if (path.trim.isEmpty) {
      Invalid(ErrorMessages.Validation.emptyField("path"))
    } else {
      val trimmedPath = path.trim
      
      // Basic path validation
      if (trimmedPath.contains("..") && !trimmedPath.startsWith("/")) {
        Invalid("Path traversal not allowed")
      } else if (mustExist && !java.nio.file.Files.exists(java.nio.file.Paths.get(trimmedPath))) {
        Invalid(ErrorMessages.IO.fileNotFound(trimmedPath))
      } else {
        Valid(trimmedPath)
      }
    }
  }
  
  /**
   * Validates count/limit parameters.
   * 
   * @param count Count value to validate
   * @param fieldName Field name for error messages
   * @param min Minimum allowed value
   * @param max Maximum allowed value
   * @return Validation result
   */
  def validateCount(count: Int, fieldName: String, min: Int = 1, max: Int = Int.MaxValue): ValidationResult[Int] = {
    if (count < min || count > max) {
      Invalid(ErrorMessages.Validation.outOfRange(fieldName, min.toDouble, max.toDouble))
    } else {
      Valid(count)
    }
  }
  
  /**
   * Validates memory size in bytes.
   * 
   * @param bytes Memory size in bytes
   * @param fieldName Field name for error messages
   * @return Validation result
   */
  def validateMemorySize(bytes: Long, fieldName: String): ValidationResult[Long] = {
    if (bytes < 0) {
      Invalid(s"$fieldName cannot be negative")
    } else if (bytes > Runtime.getRuntime.maxMemory()) {
      Invalid(s"$fieldName exceeds available memory")
    } else {
      Valid(bytes)
    }
  }
  
  /**
   * Validates duration in milliseconds.
   * 
   * @param millis Duration in milliseconds
   * @param fieldName Field name for error messages
   * @param maxMillis Maximum allowed duration
   * @return Validation result
   */
  def validateDuration(millis: Long, fieldName: String, maxMillis: Long = Long.MaxValue): ValidationResult[Long] = {
    if (millis < 0) {
      Invalid(s"$fieldName cannot be negative")
    } else if (millis > maxMillis) {
      Invalid(s"$fieldName exceeds maximum allowed duration")
    } else {
      Valid(millis)
    }
  }
  
  /**
   * Validates collection size.
   * 
   * @param collection Collection to validate
   * @param fieldName Field name for error messages
   * @param minSize Minimum size
   * @param maxSize Maximum size
   * @tparam T Collection element type
   * @return Validation result
   */
  def validateCollectionSize[T](
    collection: Iterable[T], 
    fieldName: String, 
    minSize: Int = 0, 
    maxSize: Int = Int.MaxValue
  ): ValidationResult[Iterable[T]] = {
    val size = collection.size
    if (size < minSize) {
      Invalid(s"$fieldName must contain at least $minSize items, got $size")
    } else if (size > maxSize) {
      Invalid(s"$fieldName cannot contain more than $maxSize items, got $size")
    } else {
      Valid(collection)
    }
  }
  
  /**
   * Cleans text values by removing control characters and normalizing whitespace.
   * 
   * @param text Text to clean
   * @param maxLength Maximum allowed length
   * @return Cleaned text
   */
  private def cleanTextValue(text: String, maxLength: Int): String = {
    text.trim
      .replaceAll("[\t\n\r]", " ")       // Remove tabs, newlines, carriage returns
      .replaceAll("\\s+", " ")           // Normalize multiple spaces
      .take(maxLength)                   // Truncate to max length
  }
  
  /**
   * Validates that a string matches a specific pattern.
   * 
   * @param value String to validate
   * @param pattern Regex pattern
   * @param fieldName Field name for error messages
   * @return Validation result
   */
  def validatePattern(value: String, pattern: String, fieldName: String): ValidationResult[String] = {
    if (value == null) {
      Invalid(ErrorMessages.Validation.NULL_VALUE)
    } else if (!value.matches(pattern)) {
      Invalid(ErrorMessages.Validation.invalidFormat(s"$fieldName pattern: $pattern"))
    } else {
      Valid(value)
    }
  }
  
  /**
   * Validates that a value is within an acceptable range.
   * 
   * @param value Value to validate
   * @param min Minimum value (inclusive)
   * @param max Maximum value (inclusive)
   * @param fieldName Field name for error messages
   * @return Validation result
   */
  def validateRange(value: Double, min: Double, max: Double, fieldName: String): ValidationResult[Double] = {
    if (value < min || value > max) {
      Invalid(ErrorMessages.Validation.outOfRange(fieldName, min, max))
    } else {
      Valid(value)
    }
  }
  
  /**
   * Validates that a string is not null or empty.
   * 
   * @param value String to validate
   * @param fieldName Field name for error messages
   * @return Validation result
   */
  def validateNonEmpty(value: String, fieldName: String): ValidationResult[String] = {
    if (value == null) {
      Invalid(ErrorMessages.Validation.NULL_VALUE)
    } else if (value.trim.isEmpty) {
      Invalid(ErrorMessages.Validation.emptyField(fieldName))
    } else {
      Valid(value.trim)
    }
  }
  
  /**
   * Validates that a value is not null.
   * 
   * @param value Value to validate
   * @param fieldName Field name for error messages
   * @tparam T Value type
   * @return Validation result
   */
  def validateNotNull[T](value: T, fieldName: String): ValidationResult[T] = {
    if (value == null) {
      Invalid(ErrorMessages.Validation.NULL_VALUE + s" for field: $fieldName")
    } else {
      Valid(value)
    }
  }
}