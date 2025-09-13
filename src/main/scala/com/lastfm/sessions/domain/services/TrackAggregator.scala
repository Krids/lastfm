package com.lastfm.sessions.domain.services

import com.lastfm.sessions.domain._

/**
 * Domain service for aggregating track popularity across sessions.
 * 
 * Calculates play counts, unique sessions, and unique users per track.
 * Pure domain logic with no infrastructure dependencies.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TrackAggregator {
  
  /**
   * Aggregates track popularity from ranked sessions.
   * 
   * @param sessions Sessions containing tracks to aggregate
   * @return List of track popularity statistics
   */
  def aggregateTrackPopularity(sessions: List[RankedSession]): List[TrackPopularity] = {
    if (sessions.isEmpty) {
      List.empty
    } else {
      val trackOccurrences = extractTrackOccurrences(sessions)
      val aggregated = aggregateOccurrences(trackOccurrences)
      assignRanks(aggregated)
    }
  }
  
  /**
   * Extracts all track occurrences with session and user context.
   * 
   * @param sessions Sessions to extract tracks from
   * @return List of track occurrences with metadata
   */
  private def extractTrackOccurrences(sessions: List[RankedSession]): List[TrackOccurrence] = {
    sessions.flatMap { session =>
      session.tracks.map { track =>
        TrackOccurrence(
          normalizedKey = normalizeTrackKey(track.artistName, track.trackName),
          trackName = track.trackName,
          artistName = track.artistName,
          sessionId = session.sessionId,
          userId = session.userId
        )
      }
    }
  }
  
  /**
   * Normalizes track key for aggregation.
   * Handles case sensitivity and whitespace normalization.
   * 
   * @param artistName Artist name to normalize
   * @param trackName Track name to normalize
   * @return Normalized composite key
   */
  private def normalizeTrackKey(artistName: String, trackName: String): String = {
    val normalizedArtist = artistName.trim.replaceAll("\\s+", " ").toLowerCase
    val normalizedTrack = trackName.trim.replaceAll("\\s+", " ").toLowerCase
    s"$normalizedArtist|$normalizedTrack"
  }
  
  /**
   * Aggregates track occurrences into popularity statistics.
   * 
   * @param occurrences Track occurrences to aggregate
   * @return Aggregated track popularity (unranked)
   */
  private def aggregateOccurrences(occurrences: List[TrackOccurrence]): List[TrackPopularity] = {
    occurrences
      .groupBy(_.normalizedKey)
      .map { case (_, trackOccurrences) =>
        val first = trackOccurrences.head
        TrackPopularity(
          rank = 1, // Will be reassigned later
          trackName = first.trackName,
          artistName = first.artistName,
          playCount = trackOccurrences.size,
          uniqueSessions = trackOccurrences.map(_.sessionId).distinct.size,
          uniqueUsers = trackOccurrences.map(_.userId).distinct.size
        )
      }
      .toList
  }
  
  /**
   * Assigns ranks to tracks based on popularity.
   * 
   * Ranking criteria:
   * 1. Play count (descending)
   * 2. Unique sessions (descending)
   * 3. Unique users (descending)
   * 4. Track name (lexicographic for stability)
   * 
   * @param tracks Unranked track popularity
   * @return Ranked track popularity
   */
  private def assignRanks(tracks: List[TrackPopularity]): List[TrackPopularity] = {
    val sorted = tracks.sortWith { (t1, t2) =>
      // Primary: Play count (descending)
      val playComparison = t2.playCount.compareTo(t1.playCount)
      if (playComparison != 0) {
        playComparison < 0
      } else {
        // Secondary: Unique sessions (descending)
        val sessionComparison = t2.uniqueSessions.compareTo(t1.uniqueSessions)
        if (sessionComparison != 0) {
          sessionComparison < 0
        } else {
          // Tertiary: Unique users (descending)
          val userComparison = t2.uniqueUsers.compareTo(t1.uniqueUsers)
          if (userComparison != 0) {
            userComparison < 0
          } else {
            // Quaternary: Track name (lexicographic)
            t1.trackName.compareTo(t2.trackName) < 0
          }
        }
      }
    }
    
    sorted.zipWithIndex.map { case (track, index) =>
      track.copy(rank = index + 1)
    }
  }
  
  /**
   * Gets top N tracks by popularity.
   * 
   * @param sessions Sessions to analyze
   * @param n Number of top tracks to return
   * @return Top N tracks by popularity
   */
  def getTopTracks(sessions: List[RankedSession], n: Int): List[TrackPopularity] = {
    if (n <= 0 || sessions.isEmpty) {
      List.empty
    } else {
      val allTracks = aggregateTrackPopularity(sessions)
      allTracks.take(n)
    }
  }
  
  /**
   * Calculates aggregation statistics for analysis.
   * 
   * @param sessions Sessions analyzed
   * @return Statistics about track aggregation
   */
  def calculateAggregationStatistics(sessions: List[RankedSession]): AggregationStatistics = {
    if (sessions.isEmpty) {
      AggregationStatistics(0, 0, 0, 0.0, Map.empty)
    } else {
      val allTracks = sessions.flatMap(_.tracks)
      val uniqueTracks = allTracks.map(t => normalizeTrackKey(t.artistName, t.trackName)).distinct.size
      val totalSessions = sessions.size
      
      val playDistribution = aggregateTrackPopularity(sessions)
        .groupBy(_.playCount)
        .map { case (count, tracks) => count -> tracks.size }
        
      AggregationStatistics(
        totalTracks = allTracks.size,
        uniqueTracks = uniqueTracks,
        totalSessions = totalSessions,
        averageTracksPerSession = allTracks.size.toDouble / totalSessions,
        playCountDistribution = playDistribution
      )
    }
  }
}

/**
 * Represents a single track occurrence with context.
 */
private case class TrackOccurrence(
  normalizedKey: String,
  trackName: String,
  artistName: String,
  sessionId: String,
  userId: String
)

/**
 * Statistics about track aggregation.
 */
case class AggregationStatistics(
  totalTracks: Int,
  uniqueTracks: Int,
  totalSessions: Int,
  averageTracksPerSession: Double,
  playCountDistribution: Map[Int, Int]
)

