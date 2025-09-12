package com.lastfm.sessions.domain

import java.time.Instant

/**
 * Immutable value object representing a ranked user session.
 * 
 * Core domain model for session ranking with complete track details.
 * Enforces business rules and data consistency through validation.
 * 
 * @param rank Position in ranking (1-based)
 * @param sessionId Unique session identifier
 * @param userId User identifier who created the session
 * @param trackCount Total number of tracks in session
 * @param durationMinutes Session duration in minutes
 * @param startTime Session start timestamp
 * @param endTime Session end timestamp
 * @param tracks List of tracks played in the session
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class RankedSession(
  rank: Int,
  sessionId: String,
  userId: String,
  trackCount: Int,
  durationMinutes: Long,
  startTime: Instant,
  endTime: Instant,
  tracks: List[Track]
) {
  
  // Validation: Ranking must be positive
  require(rank > 0, s"Rank must be positive, got: $rank")
  
  // Validation: Critical identifiers must be non-null and non-empty
  require(sessionId != null && sessionId.trim.nonEmpty, 
    "Session ID must not be null or empty")
  require(userId != null && userId.trim.nonEmpty, 
    "User ID must not be null or empty")
  
  // Validation: Track count must be positive and consistent
  require(trackCount >= 0, s"Track count must be non-negative, got: $trackCount")
  require(tracks != null && tracks.nonEmpty, 
    "Track list must not be null or empty")
  require(trackCount == tracks.size, 
    s"Track count ($trackCount) must match actual tracks (${tracks.size})")
  
  // Validation: Duration must be non-negative
  require(durationMinutes >= 0, 
    s"Duration must be non-negative, got: $durationMinutes minutes")
  
  // Validation: Time consistency
  require(startTime != null && endTime != null, 
    "Start and end times must not be null")
  require(!endTime.isBefore(startTime), 
    s"End time ($endTime) cannot be before start time ($startTime)")
  
  /**
   * Count of unique tracks in the session.
   * Useful for diversity analysis.
   */
  lazy val uniqueTrackCount: Int = tracks.map(_.compositeKey).distinct.size
  
  /**
   * Ratio of unique tracks to total tracks.
   * Indicates listening diversity within session.
   */
  lazy val diversityRatio: Double = 
    if (trackCount > 0) uniqueTrackCount.toDouble / trackCount else 0.0
  
  /**
   * Average track duration in minutes.
   * Calculated from session duration and track count.
   */
  lazy val averageTrackDurationMinutes: Double = 
    if (trackCount > 0) durationMinutes.toDouble / trackCount else 0.0
  
  /**
   * Checks if this session contains a specific track.
   */
  def containsTrack(track: Track): Boolean = 
    tracks.exists(t => t.compositeKey == track.compositeKey)
  
  /**
   * Gets track play frequency within this session.
   */
  def trackFrequency(track: Track): Int = 
    tracks.count(t => t.compositeKey == track.compositeKey)
  
  /**
   * Comparison for sorting sessions.
   * Primary: track count (desc), Secondary: duration (desc), Tertiary: sessionId
   */
  def compareForRanking(other: RankedSession): Int = {
    val trackComparison = other.trackCount.compareTo(this.trackCount)
    if (trackComparison != 0) trackComparison
    else {
      val durationComparison = other.durationMinutes.compareTo(this.durationMinutes)
      if (durationComparison != 0) durationComparison
      else this.sessionId.compareTo(other.sessionId)
    }
  }
}