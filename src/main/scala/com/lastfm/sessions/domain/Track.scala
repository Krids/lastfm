package com.lastfm.sessions.domain

/**
 * Immutable value object representing a music track.
 * 
 * Core domain model for track identification and aggregation.
 * Enforces business rules through validation at construction.
 * 
 * @param trackId Optional track identifier (MBID)
 * @param trackName Required track name
 * @param artistId Optional artist identifier (MBID)
 * @param artistName Required artist name
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class Track(
  trackId: String,
  trackName: String,
  artistId: String,
  artistName: String
) {
  
  // Validation: Critical fields must be non-null and non-empty
  require(trackName != null && trackName.trim.nonEmpty, 
    "Track name must not be null or empty")
  require(artistName != null && artistName.trim.nonEmpty, 
    "Artist name must not be null or empty")
  
  /**
   * Provides composite key for track identity when MBIDs are not available.
   * Format: "normalizedArtistName - normalizedTrackName"
   * Uses normalized values to ensure consistent matching.
   */
  val compositeKey: String = s"${artistName.trim.toLowerCase} - ${trackName.trim.toLowerCase}"
  
  /**
   * Determines if this track has complete MBID identification.
   */
  def hasCompleteIds: Boolean = trackId != null && artistId != null
  
  /**
   * Normalized track name for comparison (lowercase, trimmed).
   */
  lazy val normalizedTrackName: String = trackName.trim.toLowerCase
  
  /**
   * Normalized artist name for comparison (lowercase, trimmed).
   */
  lazy val normalizedArtistName: String = artistName.trim.toLowerCase
}