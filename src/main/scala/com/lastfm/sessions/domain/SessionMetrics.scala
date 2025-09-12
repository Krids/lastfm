package com.lastfm.sessions.domain

/**
 * Domain model for session analysis metrics following clean code principles.
 * 
 * Immutable value object representing aggregated session statistics from distributed processing.
 * Designed for memory efficiency and validation at construction time.
 * 
 * Key Features:
 * - Fail-fast validation of all metric values
 * - Immutable design for thread safety
 * - No infrastructure dependencies (pure domain model)
 * - Clear business rules encoded in validation
 * 
 * @param totalSessions Total number of user sessions analyzed (non-negative)
 * @param uniqueUsers Number of unique users with sessions (non-negative)
 * @param totalTracks Total number of tracks played across all sessions (non-negative)
 * @param averageSessionLength Average tracks per session (non-negative)
 * @param qualityScore Data quality score as percentage (0-100)
 * @throws IllegalArgumentException if any parameter violates business rules
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class SessionMetrics(
  totalSessions: Long,
  uniqueUsers: Long,
  totalTracks: Long,
  averageSessionLength: Double,
  qualityScore: Double
) {
  
  // Fail-fast validation at construction time (Clean Code principle)
  require(totalSessions >= 0, "totalSessions must be non-negative")
  require(uniqueUsers >= 0, "uniqueUsers must be non-negative")
  require(totalTracks >= 0, "totalTracks must be non-negative")
  require(averageSessionLength >= 0, "averageSessionLength must be non-negative")
  require(qualityScore >= 0 && qualityScore <= 100, "qualityScore must be between 0 and 100")
  
  /**
   * Calculates user engagement level based on average session length.
   * Business logic for categorizing user behavior patterns.
   */
  def engagementLevel: EngagementLevel = {
    averageSessionLength match {
      case length if length >= 50.0 => EngagementLevel.High
      case length if length >= 20.0 => EngagementLevel.Moderate
      case _ => EngagementLevel.Low
    }
  }
  
  /**
   * Determines if metrics indicate a healthy listening ecosystem.
   * Business rule: Quality > 95% and average session > 10 tracks.
   */
  def isHealthyEcosystem: Boolean = {
    qualityScore > 95.0 && averageSessionLength > 10.0
  }
  
  /**
   * Calculates tracks per user ratio for user activity analysis.
   */
  def tracksPerUser: Double = {
    if (uniqueUsers == 0) 0.0
    else totalTracks.toDouble / uniqueUsers
  }
}

/**
 * Enumeration for user engagement levels based on session patterns.
 */
sealed trait EngagementLevel

object EngagementLevel {
  case object Low extends EngagementLevel
  case object Moderate extends EngagementLevel  
  case object High extends EngagementLevel
}
