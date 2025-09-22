package com.lastfm.domain

import java.time.{Duration, Instant}
import java.util.UUID

/**
 * Core business logic for session analysis and detection.
 * 
 * This class contains pure functions that implement the 20-minute gap
 * session detection algorithm. Demonstrates:
 * - Business logic separation from infrastructure
 * - Pure functions for testability
 * - Domain-driven design principles
 * - Functional programming best practices
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object SessionAnalyzer {
  
  /**
   * Calculates user sessions from chronologically ordered listening events.
   * 
   * This is the core business algorithm that implements the 20-minute gap rule:
   * - Events within 20 minutes belong to the same session
   * - Gaps longer than 20 minutes create new sessions
   * 
   * Pure function design enables:
   * - Easy unit testing without infrastructure dependencies
   * - Predictable behavior with immutable inputs/outputs
   * - Business logic that can be validated independently
   * 
   * @param events List of listening events for a single user (must be chronologically ordered)
   * @param sessionGap Maximum gap between tracks in the same session
   * @return List of user sessions with session boundaries applied
   */
  def calculateSessions(events: List[ListenEvent], sessionGap: Duration = Duration.ofMinutes(20)): List[UserSession] = {
    if (events.isEmpty) return List.empty
    
    // Validate input is properly ordered (defensive programming)
    require(isChronologicallyOrdered(events), "Events must be chronologically ordered")
    
    val userId = events.head.userId
    
    // Simple and clear session detection algorithm
    val sessionGroups = events.foldLeft(List.empty[List[ListenEvent]]) { (sessions, event) =>
      sessions match {
        case Nil =>
          // First event - start first session
          List(List(event))
          
        case currentSession :: otherSessions =>
          val lastEventInSession = currentSession.last
          val gap = Duration.between(lastEventInSession.timestamp, event.timestamp)
          
          if (gap.compareTo(sessionGap) > 0) {
            // Gap too large - start new session
            List(event) :: sessions
          } else {
            // Add to current session
            (currentSession :+ event) :: otherSessions
          }
      }
    }
    
    // Convert to UserSession objects (reverse to get chronological order)
    sessionGroups.reverse.zipWithIndex.map { case (sessionTracks, index) =>
      UserSession(
        userId = userId,
        sessionId = generateSessionId(userId, index),
        tracks = sessionTracks
      )
    }
  }
  
  /**
   * Ranks sessions by track count in descending order.
   * 
   * This implements the business requirement to find the longest sessions
   * by track count (not duration).
   * 
   * @param sessions List of user sessions to rank
   * @return Sessions sorted by track count (most tracks first)
   */
  def rankSessionsByTrackCount(sessions: List[UserSession]): List[UserSession] = {
    sessions.sortBy(-_.trackCount)  // Negative for descending order
  }
  
  /**
   * Selects top N sessions by track count.
   * 
   * @param sessions List of sessions to select from
   * @param topN Number of top sessions to return
   * @return Top N sessions by track count
   */
  def selectTopSessions(sessions: List[UserSession], topN: Int): List[UserSession] = {
    rankSessionsByTrackCount(sessions).take(topN)
  }
  
  /**
   * Extracts all tracks from a list of sessions for popularity analysis.
   * 
   * @param sessions Sessions to extract tracks from
   * @return Flattened list of all tracks from all sessions
   */
  def extractAllTracks(sessions: List[UserSession]): List[ListenEvent] = {
    sessions.flatMap(_.tracks)
  }
  
  /**
   * Calculates track popularity from a list of listening events.
   * 
   * @param tracks List of tracks to analyze
   * @return Map of track key to play count
   */
  def calculateTrackPopularity(tracks: List[ListenEvent]): Map[String, Int] = {
    tracks.groupBy(_.trackKey).view.mapValues(_.length).toMap
  }
  
  /**
   * Selects top N most popular tracks.
   * 
   * @param trackPopularity Map of track key to play count
   * @param topN Number of top tracks to return
   * @return List of (trackKey, playCount) tuples sorted by popularity
   */
  def selectTopTracks(trackPopularity: Map[String, Int], topN: Int): List[(String, Int)] = {
    trackPopularity.toList.sortBy(-_._2).take(topN)
  }
  
  /**
   * Validates that listening events are chronologically ordered.
   * 
   * @param events List of events to validate
   * @return true if events are properly ordered by timestamp
   */
  def isChronologicallyOrdered(events: List[ListenEvent]): Boolean = {
    events.zip(events.tail).forall { case (prev, next) =>
      !prev.timestamp.isAfter(next.timestamp)
    }
  }
  
  /**
   * Generates unique session ID for a user session.
   * 
   * @param userId User identifier
   * @param sessionIndex Session index for this user
   * @return Unique session identifier
   */
  private def generateSessionId(userId: String, sessionIndex: Int): String = {
    s"${userId}_session_${sessionIndex}_${UUID.randomUUID().toString.take(8)}"
  }
}


