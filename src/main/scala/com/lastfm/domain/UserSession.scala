package com.lastfm.domain

import java.time.{Duration, Instant}

/**
 * Domain model representing a user listening session.
 * 
 * A session contains chronologically ordered tracks from a single user,
 * demonstrating:
 * - Domain-driven design principles
 * - Immutable data structures
 * - Business logic encapsulation
 * - Clean architecture separation
 * 
 * @param userId User identifier for this session
 * @param sessionId Unique identifier for this session
 * @param tracks Chronologically ordered list of listening events
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class UserSession(
  userId: String,
  sessionId: String,
  tracks: List[ListenEvent]
) {
  // Essential validation
  require(userId.nonEmpty, "userId cannot be empty")
  require(sessionId.nonEmpty, "sessionId cannot be empty")
  require(tracks.nonEmpty, "session must contain at least one track")
  
  /**
   * Total number of tracks in this session.
   * Key metric for session ranking and analysis.
   */
  lazy val trackCount: Int = tracks.length
  
  /**
   * Session start time (timestamp of first track).
   */
  lazy val startTime: Instant = tracks.head.timestamp
  
  /**
   * Session end time (timestamp of last track).
   */
  lazy val endTime: Instant = tracks.last.timestamp
  
  /**
   * Session duration calculated from first to last track.
   * Demonstrates business logic implementation in domain model.
   */
  lazy val duration: Duration = Duration.between(startTime, endTime)
  
  /**
   * Duration in minutes for easier business analysis.
   */
  lazy val durationMinutes: Double = duration.toSeconds / 60.0
  
  /**
   * Validates session has proper chronological ordering.
   * Demonstrates data quality validation at domain level.
   */
  def isChronologicallyOrdered: Boolean = {
    tracks.zip(tracks.tail).forall { case (prev, next) =>
      !prev.timestamp.isAfter(next.timestamp)
    }
  }
}


