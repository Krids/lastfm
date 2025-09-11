package com.lastfm.sessions.domain

import java.time.Duration
import scala.annotation.tailrec

/**
 * Core business logic for calculating user listening sessions.
 * 
 * Implements the 20-minute session gap algorithm:
 * - Tracks within 20 minutes of each other belong to the same session
 * - Gaps > 20 minutes create new session boundaries
 * - Sessions maintain chronological order of tracks
 * 
 * Algorithm Specification:
 * 1. Sort listening events chronologically by timestamp
 * 2. Group events by user ID for independent session calculation
 * 3. For each user's events:
 *    a. Start new session with first track
 *    b. For each subsequent track:
 *       - If gap ≤ 20 minutes: Add to current session
 *       - If gap > 20 minutes: End current session, start new session
 * 4. Return all sessions across all users
 * 
 * Key Properties:
 * - Immutable and pure functions (no side effects)
 * - Handles edge cases: empty data, single tracks, identical timestamps
 * - Efficient processing for large datasets
 * - Maintains data integrity through validation
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object SessionCalculator {
  
  // Session gap threshold: tracks more than 20 minutes apart start new sessions
  private val SESSION_GAP_MINUTES = 20
  private val SESSION_GAP_DURATION = Duration.ofMinutes(SESSION_GAP_MINUTES)

  /**
   * Calculate sessions for a single user's listening events.
   * 
   * Assumes all events belong to the same user. For mixed user data,
   * use calculateSessionsForAllUsers instead.
   * 
   * @param events Listening events for a single user (can be empty)
   * @return List of sessions ordered chronologically by start time
   */
  def calculateSessions(events: List[ListenEvent]): List[UserSession] = {
    if (events.isEmpty) return List.empty
    
    // Sort events chronologically to ensure proper session boundary detection
    val sortedEvents = events.sortBy(_.timestamp)
    
    // All events should belong to same user for this method
    val userId = sortedEvents.head.userId
    
    // Build sessions using tail-recursive algorithm
    buildSessions(sortedEvents, userId)
  }

  /**
   * Calculate sessions for multiple users' listening events.
   * 
   * Groups events by user and calculates sessions independently for each user.
   * Maintains session boundaries per user regardless of interleaved timestamps.
   * 
   * @param events Mixed listening events from multiple users
   * @return List of all sessions across all users, ordered by user ID then start time
   */
  def calculateSessionsForAllUsers(events: List[ListenEvent]): List[UserSession] = {
    if (events.isEmpty) return List.empty
    
    // Group events by user to maintain independent session boundaries
    val eventsByUser = events.groupBy(_.userId)
    
    // Calculate sessions for each user independently, then combine
    val allSessions = eventsByUser.flatMap { case (userId, userEvents) =>
      calculateSessions(userEvents)
    }.toList
    
    // Sort sessions by user ID first, then by start time for consistent ordering
    allSessions.sortBy(session => (session.userId, session.startTime))
  }

  /**
   * Core session building algorithm using tail recursion for efficiency.
   * 
   * Processes events chronologically, maintaining current session state
   * and creating new sessions when gaps exceed the 20-minute threshold.
   * 
   * @param sortedEvents Events sorted chronologically by timestamp
   * @param userId User ID for all events (assumed consistent)
   * @return List of sessions built from the events
   */
  private def buildSessions(sortedEvents: List[ListenEvent], userId: String): List[UserSession] = {
    
    @tailrec
    def buildSessionsRecursive(
      remainingEvents: List[ListenEvent],
      currentSessionTracks: List[ListenEvent],
      completedSessions: List[UserSession]
    ): List[UserSession] = {
      
      remainingEvents match {
        case Nil =>
          // No more events - finalize current session if it exists
          if (currentSessionTracks.nonEmpty) {
            val finalSession = UserSession(userId, currentSessionTracks.reverse)
            completedSessions :+ finalSession
          } else {
            completedSessions
          }
          
        case currentEvent :: restEvents =>
          if (currentSessionTracks.isEmpty) {
            // Start new session with this event
            buildSessionsRecursive(restEvents, List(currentEvent), completedSessions)
            
          } else {
            // Check gap from last track in current session to this event
            val lastTrack = currentSessionTracks.head // Note: tracks are stored in reverse order
            val gapDuration = Duration.between(lastTrack.timestamp, currentEvent.timestamp)
            
            if (gapDuration.compareTo(SESSION_GAP_DURATION) <= 0) {
              // Gap ≤ 20 minutes: Add to current session
              buildSessionsRecursive(restEvents, currentEvent :: currentSessionTracks, completedSessions)
              
            } else {
              // Gap > 20 minutes: End current session and start new one
              val completedSession = UserSession(userId, currentSessionTracks.reverse)
              val updatedCompletedSessions = completedSessions :+ completedSession
              
              // Start new session with current event
              buildSessionsRecursive(restEvents, List(currentEvent), updatedCompletedSessions)
            }
          }
      }
    }
    
    // Start recursive session building
    buildSessionsRecursive(sortedEvents, List.empty, List.empty)
  }
}

