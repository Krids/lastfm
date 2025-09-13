package com.lastfm.sessions.domain

import java.time.Instant
import scala.util.{Try, Success, Failure}

/**
 * Last.fm specific data validation functions.
 * 
 * Implements validation logic based on actual Last.fm dataset characteristics:
 * - User IDs follow "user_XXXXXX" format (6 digits)
 * - Timestamps are ISO 8601 format with perfect parsing success
 * - Track names occasionally empty (8 out of 19M records)
 * - Artist names include Unicode characters (坂本龍一, Sigur Rós)
 * - Track identity resolution: MBID preferred, fallback to artist+track
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object LastFmDataValidation {

  // Regex pattern for Last.fm user ID format: "user_" followed by exactly 6 digits
  private val UserIdPattern = """^user_\d{6}$""".r

  /**
   * Validates Last.fm user ID format.
   * 
   * Based on actual dataset analysis showing all user IDs follow "user_XXXXXX" format.
   * 
   * @param userId The user ID to validate
   * @return Valid(cleanUserId) or Invalid(reason)
   */
  def validateUserId(userId: String): ValidationResult[String] = {
    if (userId == null) {
      Invalid("userId cannot be null")
    } else if (userId.isEmpty) {
      Invalid("userId cannot be empty")
    } else if (userId.isBlank) {
      Invalid("userId cannot be blank")
    } else {
      val trimmedUserId = userId.trim
      if (!UserIdPattern.matches(trimmedUserId)) {
        Invalid("userId must follow user_XXXXXX format")
      } else {
        Valid(trimmedUserId)
      }
    }
  }

  /**
   * Validates and parses ISO 8601 timestamp.
   * 
   * Based on dataset analysis showing perfect timestamp parsing success.
   * Normalizes all timestamps to UTC for consistent session analysis.
   * 
   * @param timestampStr The timestamp string to validate and parse
   * @return Valid(Instant) or Invalid(reason)
   */
  def validateTimestamp(timestampStr: String): ValidationResult[Instant] = {
    if (timestampStr == null) {
      Invalid("timestamp cannot be null")
    } else {
      val trimmedTimestamp = timestampStr.trim
      if (trimmedTimestamp.isEmpty) {
        Invalid("timestamp cannot be empty")
      } else {
        Try(Instant.parse(trimmedTimestamp)) match {
          case Success(instant) => Valid(instant)
          case Failure(_) => Invalid("Cannot parse timestamp")
        }
      }
    }
  }

  /**
   * Validates and cleans track name.
   * 
   * Based on dataset analysis showing 8 empty track names out of 19M records.
   * These are replaced with "Unknown Track" to maintain data completeness.
   * 
   * @param trackName The track name to validate and clean
   * @return Valid(cleanTrackName) - never fails, uses default for empty
   */
  def validateTrackName(trackName: String): ValidationResult[String] = {
    val cleanName = Option(trackName)
      .filter(_.trim.nonEmpty)
      .map(_.trim)
      .getOrElse("Unknown Track")
    
    Valid(cleanName)
  }

  /**
   * Validates and cleans artist name.
   * 
   * Based on dataset analysis showing no empty artist names but need to handle
   * Unicode characters (坂本龍一, Sigur Rós) and potential nulls defensively.
   * 
   * @param artistName The artist name to validate and clean
   * @return Valid(cleanArtistName) - never fails, uses default for empty
   */
  def validateArtistName(artistName: String): ValidationResult[String] = {
    val cleanName = Option(artistName)
      .filter(_.trim.nonEmpty)
      .map(_.trim)
      .getOrElse("Unknown Artist")
    
    Valid(cleanName)
  }

  /**
   * Generates deterministic track key for identity resolution.
   * 
   * Strategy based on dataset analysis:
   * 1. Use track MBID if available (88.7% coverage)
   * 2. Fall back to "artist — track" combination (11.3% cases)
   * 
   * The "—" separator (em dash) is used to minimize collision risk
   * with actual artist or track names containing hyphens.
   * 
   * @param trackId Optional track MBID
   * @param artistName Artist name (will be cleaned)
   * @param trackName Track name (will be cleaned)
   * @return Deterministic track key string
   */
  def generateTrackKey(
    trackId: Option[String],
    artistName: String,
    trackName: String
  ): String = {
    // Check if trackId is present and non-empty
    val effectiveTrackId = trackId.filter(id => id != null && id.trim.nonEmpty)
    
    effectiveTrackId match {
      case Some(mbid) => mbid.trim
      case None =>
        // Fall back to artist — track combination using cleaned names
        val cleanArtist = validateArtistName(artistName) match {
          case Valid(name) => name
          case Invalid(_) => "Unknown Artist" // Should never happen since validateArtistName never fails
        }
        val cleanTrack = validateTrackName(trackName) match {
          case Valid(name) => name
          case Invalid(_) => "Unknown Track" // Should never happen since validateTrackName never fails
        }
        s"$cleanArtist — $cleanTrack"
    }
  }
}