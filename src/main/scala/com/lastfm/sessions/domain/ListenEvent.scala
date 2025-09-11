package com.lastfm.sessions.domain

import java.time.Instant

/**
 * Domain model representing a single listening event from Last.fm data.
 * 
 * Enhanced with data quality improvements and track identity resolution.
 * Validates all critical fields at construction time to ensure data integrity.
 * 
 * @param userId Unique identifier for the user (required, non-null, non-empty)
 * @param timestamp When the track was played (required, non-null)
 * @param artistId Optional MusicBrainz artist ID
 * @param artistName Name of the artist (required, non-null, non-empty)
 * @param trackId Optional MusicBrainz track ID  
 * @param trackName Name of the track (required, non-null, non-empty)
 * @param trackKey Deterministic track identifier (MBID preferred, fallback to "artist — track")
 * @throws IllegalArgumentException if any required field is null, empty, or blank
 */
case class ListenEvent(
  userId: String,
  timestamp: Instant,
  artistId: Option[String],
  artistName: String,
  trackId: Option[String],
  trackName: String,
  trackKey: String
) {
  // Validate critical fields at construction time
  require(userId != null, "userId cannot be null")
  require(userId.nonEmpty, "userId cannot be empty")
  require(!userId.isBlank, "userId cannot be blank")
  
  require(timestamp != null, "timestamp cannot be null")
  
  require(artistName != null, "artistName cannot be null")
  require(artistName.nonEmpty, "artistName cannot be empty")
  require(!artistName.isBlank, "artistName cannot be blank")
  
  require(trackName != null, "trackName cannot be null")
  require(trackName.nonEmpty, "trackName cannot be empty")
  require(!trackName.isBlank, "trackName cannot be blank")
  
  require(trackKey != null, "trackKey cannot be null")
  require(trackKey.nonEmpty, "trackKey cannot be empty")
  require(!trackKey.isBlank, "trackKey cannot be blank")
}

object ListenEvent {
  /**
   * Creates a minimal listen event with only required fields.
   * 
   * Automatically generates trackKey using the fallback strategy (artist — track)
   * since no MBIDs are provided.
   * 
   * @param userId Unique identifier for the user
   * @param timestamp When the track was played
   * @param artistName Name of the artist
   * @param trackName Name of the track
   * @return Validated ListenEvent instance with generated trackKey
   * @throws IllegalArgumentException if validation fails
   */
  def minimal(
    userId: String,
    timestamp: Instant,
    artistName: String,
    trackName: String
  ): ListenEvent = {
    val trackKey = LastFmDataValidation.generateTrackKey(None, artistName, trackName)
    
    ListenEvent(
      userId = userId,
      timestamp = timestamp,
      artistId = None,
      artistName = artistName,
      trackId = None,
      trackName = trackName,
      trackKey = trackKey
    )
  }
}