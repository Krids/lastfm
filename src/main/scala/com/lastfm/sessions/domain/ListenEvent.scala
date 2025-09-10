package com.lastfm.sessions.domain

import java.time.Instant

/**
 * Domain model representing a single listening event from Last.fm data.
 * 
 * Validates all critical fields at construction time to ensure data integrity.
 * 
 * @param userId Unique identifier for the user (required, non-null, non-empty)
 * @param timestamp When the track was played (required, non-null)
 * @param artistId Optional MusicBrainz artist ID
 * @param artistName Name of the artist (required, non-null, non-empty)
 * @param trackId Optional MusicBrainz track ID  
 * @param trackName Name of the track (required, non-null, non-empty)
 * @throws IllegalArgumentException if any required field is null, empty, or blank
 */
case class ListenEvent(
  userId: String,
  timestamp: Instant,
  artistId: Option[String],
  artistName: String,
  trackId: Option[String],
  trackName: String
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
}

object ListenEvent {
  /**
   * Creates a minimal listen event with only required fields.
   * 
   * @param userId Unique identifier for the user
   * @param timestamp When the track was played
   * @param artistName Name of the artist
   * @param trackName Name of the track
   * @return Validated ListenEvent instance
   * @throws IllegalArgumentException if validation fails
   */
  def minimal(
    userId: String,
    timestamp: Instant,
    artistName: String,
    trackName: String
  ): ListenEvent = {
    ListenEvent(
      userId = userId,
      timestamp = timestamp,
      artistId = None,
      artistName = artistName,
      trackId = None,
      trackName = trackName
    )
  }
}