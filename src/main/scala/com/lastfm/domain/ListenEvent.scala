package com.lastfm.domain

import java.time.Instant

/**
 * Core domain model representing a single music listening event.
 * 
 * Immutable value object with essential validation demonstrating:
 * - Clean domain modeling principles
 * - Type safety and data integrity 
 * - Functional programming best practices
 * 
 * @param userId Unique identifier for the user
 * @param timestamp When the track was played
 * @param artistName Name of the artist
 * @param trackName Name of the track
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class ListenEvent(
  userId: String,
  timestamp: Instant,
  artistName: String,
  trackName: String
) {
  // Essential validation demonstrating defensive programming
  require(userId.nonEmpty, "userId cannot be empty")
  require(artistName.nonEmpty, "artistName cannot be empty")
  require(trackName.nonEmpty, "trackName cannot be empty")
  
  /**
   * Creates a track key for deduplication and identity resolution.
   * This demonstrates data engineering best practices for identity management.
   */
  lazy val trackKey: String = s"$artistName - $trackName"
}