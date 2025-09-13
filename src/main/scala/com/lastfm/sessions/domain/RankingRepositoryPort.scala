package com.lastfm.sessions.domain

import scala.util.Try

/**
 * Port interface for ranking repository operations.
 * 
 * Defines contract for data access in the ranking domain.
 * Part of hexagonal architecture - decouples domain from infrastructure.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait RankingRepositoryPort {
  
  /**
   * Loads sessions from Silver layer storage.
   * 
   * @param path Path to Silver layer session data
   * @return Try containing list of sessions or error
   */
  def loadSessions(path: String): Try[List[SessionData]]
  
  /**
   * Loads session metadata from Gold layer.
   * 
   * @param path Path to Gold layer metadata
   * @return Try containing session metadata or error
   */
  def loadSessionMetadata(path: String): Try[SessionMetadata]
  
  /**
   * Persists ranking results to storage.
   * 
   * @param result Ranking results to persist
   * @param path Output path for results
   * @return Try indicating success or failure
   */
  def saveRankingResults(result: RankingResult, path: String): Try[Unit]
  
  /**
   * Generates TSV output file.
   * 
   * @param tracks Top tracks to output
   * @param outputPath Path for TSV file
   * @return Try indicating success or failure
   */
  def generateTsvOutput(tracks: List[TrackPopularity], outputPath: String): Try[Unit]
}

/**
 * Represents session data from storage.
 * Intermediate representation before domain conversion.
 */
case class SessionData(
  sessionId: String,
  userId: String,
  trackCount: Int,
  durationMinutes: Long,
  startTime: String,
  endTime: String,
  tracks: List[TrackData]
)

/**
 * Represents track data from storage.
 */
case class TrackData(
  trackId: Option[String],
  trackName: String,
  artistId: Option[String],
  artistName: String
)

/**
 * Session metadata from Gold layer.
 */
case class SessionMetadata(
  totalSessions: Int,
  totalTracks: Int,
  processingTime: Long,
  qualityScore: Double
)