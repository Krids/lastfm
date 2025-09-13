package com.lastfm.sessions.domain

/**
 * Immutable value object representing track popularity statistics.
 * 
 * Aggregates track play statistics across selected sessions.
 * Enforces logical consistency through validation.
 * 
 * @param rank Position in popularity ranking (1-based)
 * @param trackName Name of the track
 * @param artistName Name of the artist
 * @param playCount Total number of times played
 * @param uniqueSessions Number of unique sessions containing track
 * @param uniqueUsers Number of unique users who played track
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class TrackPopularity(
  rank: Int,
  trackName: String,
  artistName: String,
  playCount: Int,
  uniqueSessions: Int,
  uniqueUsers: Int
) {
  
  // Validation: Ranking must be positive
  require(rank > 0, s"Rank must be positive, got: $rank")
  
  // Validation: Track and artist names must be non-null and non-empty
  require(trackName != null && trackName.trim.nonEmpty, 
    "Track name must not be null or empty")
  require(artistName != null && artistName.trim.nonEmpty, 
    "Artist name must not be null or empty")
  
  // Validation: Counts must be non-negative
  require(playCount >= 0, s"Play count must be non-negative, got: $playCount")
  require(uniqueSessions >= 0, s"Unique sessions must be non-negative, got: $uniqueSessions")
  require(uniqueUsers >= 0, s"Unique users must be non-negative, got: $uniqueUsers")
  
  // Validation: Logical consistency between counts
  require(playCount >= uniqueSessions, 
    s"Play count ($playCount) cannot be less than unique sessions ($uniqueSessions)")
  require(uniqueSessions >= uniqueUsers, 
    s"Unique sessions ($uniqueSessions) cannot be less than unique users ($uniqueUsers)")
  
  // Special case: if play count is 0, sessions and users must also be 0
  if (playCount == 0) {
    require(uniqueSessions == 0 && uniqueUsers == 0, 
      "Zero play count requires zero sessions and users")
  }
  
  /**
   * Composite key for track identity.
   * Format: "artistName - trackName"
   */
  val compositeKey: String = s"$artistName - $trackName"
  
  /**
   * Average number of plays per session.
   * Indicates track repetition within sessions.
   */
  def averagePlaysPerSession: Double = 
    if (uniqueSessions > 0) playCount.toDouble / uniqueSessions else 0.0
  
  /**
   * Average number of plays per user.
   * Indicates user affinity for the track.
   */
  def averagePlaysPerUser: Double = 
    if (uniqueUsers > 0) playCount.toDouble / uniqueUsers else 0.0
  
  /**
   * Popularity score combining multiple factors.
   * Higher score indicates more popular track.
   */
  def popularityScore: Double = {
    // Weighted combination of play count, session spread, and user reach
    val playWeight = 0.5
    val sessionWeight = 0.3
    val userWeight = 0.2
    
    (playCount * playWeight) + 
    (uniqueSessions * sessionWeight) + 
    (uniqueUsers * userWeight)
  }
  
  /**
   * Formats track for TSV output.
   * Escapes special characters properly.
   */
  def toTsvRow: String = {
    val escapedTrackName = trackName.replaceAll("[\t\n\r]", " ")
    val escapedArtistName = artistName.replaceAll("[\t\n\r]", " ")
    s"$rank\t$escapedTrackName\t$escapedArtistName\t$playCount"
  }
}