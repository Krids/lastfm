package com.lastfm.sessions.domain.services

import com.lastfm.sessions.domain.RankedSession

/**
 * Domain service for ranking sessions.
 * 
 * Implements deterministic ranking algorithm with multi-level tie-breaking.
 * Pure domain logic with no infrastructure dependencies.
 * 
 * Ranking criteria (in order):
 * 1. Track count (descending)
 * 2. Duration (descending)
 * 3. Start time (ascending - earliest first)
 * 4. Session ID (lexicographic - for stability)
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionRanker {
  
  /**
   * Ranks sessions according to business rules.
   * 
   * @param sessions List of sessions to rank
   * @return Ranked sessions with assigned positions
   */
  def rankSessions(sessions: List[RankedSession]): List[RankedSession] = {
    if (sessions.isEmpty) {
      List.empty
    } else {
      val sorted = sessions.sortWith(compareSessionsForRanking)
      assignRanks(sorted)
    }
  }
  
  /**
   * Gets top N sessions by rank.
   * 
   * @param sessions Sessions to rank and filter
   * @param n Number of top sessions to return
   * @return Top N ranked sessions
   */
  def getTopSessions(sessions: List[RankedSession], n: Int): List[RankedSession] = {
    if (n <= 0 || sessions.isEmpty) {
      List.empty
    } else {
      val ranked = rankSessions(sessions)
      ranked.take(n)
    }
  }
  
  /**
   * Comparison function for deterministic session ranking.
   * 
   * @param s1 First session
   * @param s2 Second session
   * @return true if s1 should rank higher than s2
   */
  private def compareSessionsForRanking(s1: RankedSession, s2: RankedSession): Boolean = {
    // Primary: Track count (descending)
    val trackComparison = s2.trackCount.compareTo(s1.trackCount)
    if (trackComparison != 0) {
      return trackComparison < 0
    }
    
    // Secondary: Duration (descending)
    val durationComparison = s2.durationMinutes.compareTo(s1.durationMinutes)
    if (durationComparison != 0) {
      return durationComparison < 0
    }
    
    // Tertiary: Start time (ascending - earliest first)
    val startTimeComparison = s1.startTime.compareTo(s2.startTime)
    if (startTimeComparison != 0) {
      return startTimeComparison < 0
    }
    
    // Quaternary: Session ID (lexicographic for stability)
    s1.sessionId.compareTo(s2.sessionId) < 0
  }
  
  /**
   * Assigns sequential rank positions to sorted sessions.
   * 
   * @param sortedSessions Sessions already sorted by ranking criteria
   * @return Sessions with assigned rank positions
   */
  private def assignRanks(sortedSessions: List[RankedSession]): List[RankedSession] = {
    sortedSessions.zipWithIndex.map { case (session, index) =>
      session.copy(rank = index + 1)
    }
  }
  
  /**
   * Calculates ranking statistics for analysis.
   * 
   * @param sessions Ranked sessions
   * @return Statistics about the ranking distribution
   */
  def calculateRankingStatistics(sessions: List[RankedSession]): RankingStatistics = {
    if (sessions.isEmpty) {
      RankingStatistics(0, 0, 0, 0.0, 0.0)
    } else {
      val trackCounts = sessions.map(_.trackCount)
      val durations = sessions.map(_.durationMinutes)
      
      RankingStatistics(
        totalSessions = sessions.size,
        maxTrackCount = trackCounts.max,
        minTrackCount = trackCounts.min,
        averageTrackCount = trackCounts.sum.toDouble / trackCounts.size,
        averageDuration = durations.sum.toDouble / durations.size
      )
    }
  }
}

/**
 * Statistics about session ranking distribution.
 */
case class RankingStatistics(
  totalSessions: Int,
  maxTrackCount: Int,
  minTrackCount: Int,
  averageTrackCount: Double,
  averageDuration: Double
)