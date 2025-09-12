package com.lastfm.sessions.domain

import com.lastfm.sessions.common.{Constants, ConfigurableConstants}
import com.lastfm.sessions.common.traits.MetricsCalculator

/**
 * Immutable value object representing final ranking results.
 * 
 * Aggregates top sessions and tracks with processing metadata.
 * Provides comprehensive audit trail and quality metrics.
 * 
 * @param topSessions Ranked list of top sessions (configurable limit)
 * @param topTracks Ranked list of top tracks (configurable limit)
 * @param totalSessionsAnalyzed Total number of sessions processed
 * @param totalTracksAnalyzed Total number of tracks processed
 * @param processingTimeMillis Processing duration in milliseconds
 * @param qualityScore Data quality score (0-100)
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class RankingResult(
  topSessions: List[RankedSession],
  topTracks: List[TrackPopularity],
  totalSessionsAnalyzed: Int,
  totalTracksAnalyzed: Int,
  processingTimeMillis: Long,
  qualityScore: Double
) {
  
  // Validation: Collections must not be null
  require(topSessions != null, "Top sessions must not be null")
  require(topTracks != null, "Top tracks must not be null")
  
  // Validation: Size constraints per business requirements
  require(topSessions.size <= ConfigurableConstants.SessionAnalysis.TOP_SESSIONS_COUNT.value, 
    s"Top sessions must not exceed ${ConfigurableConstants.SessionAnalysis.TOP_SESSIONS_COUNT.value}, got: ${topSessions.size}")
  require(topTracks.size <= ConfigurableConstants.SessionAnalysis.TOP_TRACKS_COUNT.value, 
    s"Top tracks must not exceed ${ConfigurableConstants.SessionAnalysis.TOP_TRACKS_COUNT.value}, got: ${topTracks.size}")
  
  // Validation: Counts must be non-negative
  require(totalSessionsAnalyzed >= 0, 
    s"Total sessions must be non-negative, got: $totalSessionsAnalyzed")
  require(totalTracksAnalyzed >= 0, 
    s"Total tracks must be non-negative, got: $totalTracksAnalyzed")
  
  // Validation: Processing time must be non-negative
  require(processingTimeMillis >= 0, 
    s"Processing time must be non-negative, got: $processingTimeMillis")
  
  // Validation: Quality score must be between 0 and 100
  require(qualityScore >= 0.0 && qualityScore <= 100.0, 
    s"Quality score must be between 0 and 100, got: $qualityScore")
  
  /**
   * Processing time in seconds for readability.
   */
  def processingTimeSeconds: Double = processingTimeMillis / 1000.0
  
  /**
   * Processing throughput in tracks per second.
   */
  def tracksPerSecond: Double = 
    if (processingTimeMillis > 0) {
      (totalTracksAnalyzed * 1000.0) / processingTimeMillis
    } else 0.0
  
  /**
   * Average number of sessions per top track.
   */
  def averageSessionsPerTrack: Double = 
    if (topTracks.nonEmpty) {
      totalSessionsAnalyzed.toDouble / topTracks.size
    } else 0.0
  
  /**
   * Data compression ratio (total tracks to top N tracks).
   */
  def compressionRatio: Double = 
    if (topTracks.nonEmpty) {
      totalTracksAnalyzed.toDouble / ConfigurableConstants.SessionAnalysis.TOP_TRACKS_COUNT.value
    } else 0.0
  
  /**
   * Determines if results meet quality threshold.
   */
  def isHighQuality: Boolean = qualityScore >= ConfigurableConstants.DataQuality.SESSION_ANALYSIS_MIN_QUALITY.value
  
  /**
   * Generates comprehensive audit summary.
   */
  def auditSummary: String = {
    s"""Ranking Result Audit:
       |Top Sessions: ${topSessions.size}
       |Top Tracks: ${topTracks.size}
       |Total Sessions Analyzed: $totalSessionsAnalyzed
       |Total Tracks Analyzed: $totalTracksAnalyzed
       |Processing Time: $processingTimeSeconds seconds
       |Throughput: ${"%.2f".format(tracksPerSecond)} tracks/second
       |Quality Score: $qualityScore%
       |Quality Status: ${if (isHighQuality) "HIGH" else "NORMAL"}
       |""".stripMargin
  }
  
  /**
   * Generates summary statistics for monitoring.
   */
  def toMetrics: Map[String, Any] = Map(
    "topSessionsCount" -> topSessions.size,
    "topTracksCount" -> topTracks.size,
    "totalSessionsAnalyzed" -> totalSessionsAnalyzed,
    "totalTracksAnalyzed" -> totalTracksAnalyzed,
    "processingTimeMillis" -> processingTimeMillis,
    "tracksPerSecond" -> tracksPerSecond,
    "qualityScore" -> qualityScore,
    "isHighQuality" -> isHighQuality
  )
}