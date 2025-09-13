package com.lastfm.sessions.domain

import java.time.{Duration, Instant}

/**
 * Domain model representing a user listening session.
 * 
 * A session contains chronologically ordered tracks from a single user,
 * with session boundaries determined by temporal gaps in listening history.
 * 
 * Key Properties:
 * - Immutable value object with validation at construction
 * - Tracks are maintained in chronological order
 * - Duration calculated from first track to last track timestamp
 * - Provides both total plays and unique track counts
 * 
 * @param userId Unique identifier for the user (required, non-null, non-empty)
 * @param tracks Chronologically ordered list of tracks in session (required, non-empty)
 * @throws IllegalArgumentException if any required field is null, empty, or invalid
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class UserSession(
  userId: String,
  tracks: List[ListenEvent]
) {
  
  // Validate required fields at construction time (Fail Fast principle)
  require(userId != null, "userId cannot be null")
  require(userId.nonEmpty, "userId cannot be empty")
  require(!userId.isBlank, "userId cannot be blank")
  require(tracks != null, "tracks cannot be null")
  require(tracks.nonEmpty, "tracks cannot be empty")

  /**
   * Total number of tracks played in this session.
   */
  lazy val trackCount: Int = tracks.length

  /**
   * Number of unique tracks played in this session.
   * Multiple plays of the same track are counted as one unique track.
   */
  lazy val uniqueTrackCount: Int = tracks.map(_.trackKey).toSet.size

  /**
   * Session duration calculated as time difference between first and last track.
   * For single-track sessions, duration is zero.
   */
  lazy val duration: Duration = {
    if (tracks.length <= 1) {
      Duration.ZERO
    } else {
      Duration.between(tracks.head.timestamp, tracks.last.timestamp)
    }
  }

  /**
   * Timestamp when the session started (first track timestamp).
   */
  lazy val startTime: Instant = tracks.head.timestamp

  /**
   * Timestamp when the session ended (last track timestamp).
   * For single-track sessions, this equals startTime.
   */
  lazy val endTime: Instant = tracks.last.timestamp
}

