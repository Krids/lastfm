package com.lastfm.sessions.domain

import java.time.Duration
import scala.annotation.tailrec

/**
 * Pure domain logic for detecting session boundaries in listening events.
 * 
 * Implements the core business rule: tracks with gaps > session threshold start new sessions.
 * This is pure domain logic with no infrastructure dependencies, making it easily testable
 * and following clean code principles.
 * 
 * Key Features:
 * - Pure functions with no side effects
 * - Immutable data processing
 * - Clear business logic separation
 * - Efficient tail-recursive implementation
 * - Comprehensive edge case handling
 * 
 * Algorithm:
 * 1. Process events in given order (sorting is caller's responsibility)
 * 2. First event always starts a new session
 * 3. Compare each event with previous event timestamp
 * 4. If gap exceeds threshold, mark as new session boundary
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object SessionBoundaryDetector {
  
  /**
   * Detects session boundaries based on time gaps between consecutive events.
   * 
   * Pure function that identifies indices where new sessions should start.
   * Processes events in the order provided - sorting is caller's responsibility.
   * 
   * @param events List of listening events (can be empty)
   * @param sessionGap Maximum time gap within a session
   * @return List of indices where new sessions start (0-based)
   */
  def detectBoundaries(events: List[ListenEvent], sessionGap: Duration): List[Int] = {
    if (events.isEmpty) return List.empty
    
    // First event always starts a session
    val firstBoundary = List(0)
    
    if (events.length == 1) return firstBoundary
    
    // Find additional boundaries using tail recursion
    val additionalBoundaries = findBoundariesRecursive(events, sessionGap, 1, List.empty)
    
    firstBoundary ++ additionalBoundaries
  }
  
  /**
   * Tail-recursive helper for finding session boundaries.
   * 
   * @param events All events being processed
   * @param sessionGap Session gap threshold
   * @param currentIndex Current index being examined
   * @param boundaries Accumulated boundary indices
   * @return List of boundary indices found
   */
  @tailrec
  private def findBoundariesRecursive(
    events: List[ListenEvent], 
    sessionGap: Duration, 
    currentIndex: Int, 
    boundaries: List[Int]
  ): List[Int] = {
    
    if (currentIndex >= events.length) {
      boundaries.reverse // Return in correct order
    } else {
      val currentEvent = events(currentIndex)
      val previousEvent = events(currentIndex - 1)
      
      val isNewSessionBoundary = isNewSession(previousEvent, currentEvent, sessionGap)
      
      val updatedBoundaries = if (isNewSessionBoundary) {
        currentIndex :: boundaries
      } else {
        boundaries
      }
      
      findBoundariesRecursive(events, sessionGap, currentIndex + 1, updatedBoundaries)
    }
  }
  
  /**
   * Determines if current event starts a new session based on time gap.
   * 
   * Business rule: Gap greater than session threshold indicates new session.
   * 
   * @param previousEvent Previous event in sequence
   * @param currentEvent Current event being evaluated
   * @param sessionGap Maximum allowed gap within session
   * @return True if current event starts new session
   */
  private def isNewSession(
    previousEvent: ListenEvent, 
    currentEvent: ListenEvent, 
    sessionGap: Duration
  ): Boolean = {
    val timeBetween = Duration.between(previousEvent.timestamp, currentEvent.timestamp)
    timeBetween.compareTo(sessionGap) > 0
  }
}
