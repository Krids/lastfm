package com.lastfm.sessions.domain

/**
 * Domain model for comprehensive session analysis and statistics.
 * 
 * Provides aggregation, ranking, and statistical analysis of user listening sessions.
 * Designed for analyzing large collections of sessions to identify patterns,
 * top sessions, and user behavior insights.
 * 
 * Key Features:
 * - Session ranking by track count with consistent tie handling
 * - Statistical analysis (averages, distributions, quality metrics)
 * - Top-N session extraction with boundary condition handling
 * - User activity pattern analysis
 * - Immutable value object with comprehensive validation
 * 
 * @param sessions Collection of user sessions to analyze (required, can be empty but not null)
 * @throws IllegalArgumentException if sessions parameter is null
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class SessionAnalysis(sessions: List[UserSession]) {
  
  // Validate required fields at construction time (Fail Fast principle)
  require(sessions != null, "sessions cannot be null")
  
  /**
   * Total number of sessions in the analysis.
   */
  lazy val totalSessions: Int = sessions.length
  
  /**
   * Number of unique users across all sessions.
   */
  lazy val uniqueUsers: Int = sessions.map(_.userId).toSet.size
  
  /**
   * Total number of tracks played across all sessions.
   */
  lazy val totalTracks: Int = sessions.map(_.trackCount).sum
  
  /**
   * Average number of tracks per session.
   * Returns 0.0 for empty session collections.
   */
  lazy val averageSessionLength: Double = {
    if (sessions.isEmpty) 0.0
    else totalTracks.toDouble / totalSessions
  }
  
  /**
   * Session with the highest track count.
   * Throws NoSuchElementException if sessions is empty.
   */
  lazy val longestSession: UserSession = sessions.maxBy(_.trackCount)
  
  /**
   * Session with the lowest track count.
   * Throws NoSuchElementException if sessions is empty.
   */
  lazy val shortestSession: UserSession = sessions.minBy(_.trackCount)
  
  /**
   * Sessions ordered by track count descending, then by user ID for consistent tie handling.
   */
  lazy val sessionsByTrackCount: List[UserSession] = {
    sessions.sortBy(session => (-session.trackCount, session.userId))
  }
  
  /**
   * Extract top N sessions by track count.
   * 
   * @param n Number of top sessions to return (must be non-negative)
   * @return List of top N sessions, or all sessions if N > total sessions
   */
  def topSessions(n: Int): List[UserSession] = {
    require(n >= 0, "n must be non-negative")
    sessionsByTrackCount.take(n)
  }
  
  /**
   * Distribution of sessions by track count.
   * Map from track count to number of sessions with that count.
   */
  lazy val sessionDistribution: Map[Int, Int] = {
    sessions.groupBy(_.trackCount).view.mapValues(_.length).toMap
  }
  
  /**
   * User activity statistics mapping user ID to their session statistics.
   */
  lazy val userStatistics: Map[String, UserStatistics] = {
    sessions.groupBy(_.userId).view.mapValues { userSessions =>
      UserStatistics(
        sessionCount = userSessions.length,
        totalTracks = userSessions.map(_.trackCount).sum,
        averageSessionLength = userSessions.map(_.trackCount).sum.toDouble / userSessions.length
      )
    }.toMap
  }
  
  /**
   * Number of sessions considered "high quality" (>40 tracks).
   * This threshold represents sessions with substantial listening activity.
   */
  lazy val highQualitySessionCount: Int = sessions.count(_.trackCount > 40)
  
  /**
   * Overall quality score based on session length distribution (0-100%).
   * Higher scores indicate more substantial listening sessions.
   */
  lazy val qualityScore: Double = {
    if (sessions.isEmpty) 0.0
    else (highQualitySessionCount.toDouble / totalSessions) * 100.0
  }
}

/**
 * Statistics for a specific user's listening behavior.
 * 
 * @param sessionCount Number of sessions for this user
 * @param totalTracks Total tracks played across all user sessions
 * @param averageSessionLength Average tracks per session for this user
 */
case class UserStatistics(
  sessionCount: Int,
  totalTracks: Int,
  averageSessionLength: Double
) {
  require(sessionCount >= 0, "sessionCount must be non-negative")
  require(totalTracks >= 0, "totalTracks must be non-negative")
  require(averageSessionLength >= 0.0, "averageSessionLength must be non-negative")
}

