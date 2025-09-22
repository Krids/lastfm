package com.lastfm.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.{Instant, Duration}

/**
 * Tests for the SessionAnalyzer business logic.
 * 
 * Validates the core 20-minute gap session detection algorithm.
 * This is the critical business logic that must be thoroughly tested.
 */
class SessionAnalyzerSpec extends AnyFlatSpec with Matchers {
  
  private def createEvent(userId: String, timestamp: String, artist: String, track: String): ListenEvent = {
    ListenEvent(userId, Instant.parse(timestamp), artist, track)
  }
  
  "SessionAnalyzer" should "create single session for consecutive tracks within gap" in {
    // Arrange - Events within 20 minutes
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00Z", "Artist1", "Track1"),
      createEvent("user1", "2023-01-01T10:15:00Z", "Artist2", "Track2"),  // 15 minutes later
      createEvent("user1", "2023-01-01T10:25:00Z", "Artist3", "Track3")   // 10 minutes later
    )
    
    // Act
    val sessions = SessionAnalyzer.calculateSessions(events)
    
    // Assert
    sessions should have length 1
    sessions.head.trackCount should be(3)
  }
  
  it should "create separate sessions when gap exceeds 20 minutes" in {
    // Arrange - Events with 25-minute gap
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00Z", "Artist1", "Track1"),
      createEvent("user1", "2023-01-01T10:25:00Z", "Artist2", "Track2")  // 25 minutes later
    )
    
    // Act
    val sessions = SessionAnalyzer.calculateSessions(events)
    
    // Assert
    sessions should have length 2
    sessions.foreach(_.trackCount should be(1))
  }
  
  it should "handle single track session" in {
    // Arrange
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00Z", "Artist1", "Track1")
    )
    
    // Act
    val sessions = SessionAnalyzer.calculateSessions(events)
    
    // Assert
    sessions should have length 1
    sessions.head.trackCount should be(1)
    sessions.head.duration should be(Duration.ZERO)
  }
  
  it should "handle empty events list" in {
    // Arrange
    val events = List.empty[ListenEvent]
    
    // Act
    val sessions = SessionAnalyzer.calculateSessions(events)
    
    // Assert
    sessions should be(empty)
  }
  
  it should "rank sessions by track count in descending order" in {
    // Arrange
    val session1 = UserSession("user1", "s1", List(createEvent("user1", "2023-01-01T10:00:00Z", "A1", "T1")))
    val session2 = UserSession("user1", "s2", List(
      createEvent("user1", "2023-01-01T11:00:00Z", "A2", "T2"),
      createEvent("user1", "2023-01-01T11:05:00Z", "A3", "T3"),
      createEvent("user1", "2023-01-01T11:10:00Z", "A4", "T4")
    ))
    val sessions = List(session1, session2)
    
    // Act
    val ranked = SessionAnalyzer.rankSessionsByTrackCount(sessions)
    
    // Assert
    ranked.head.trackCount should be(3)  // session2 first
    ranked.last.trackCount should be(1)  // session1 last
  }
  
  it should "select top N sessions correctly" in {
    // Arrange
    val sessions = (1 to 5).map { i =>
      val tracks = (1 to i).map(j => createEvent(s"user1", s"2023-01-01T${10 + j}:00:00Z", s"Artist$j", s"Track$j")).toList
      UserSession("user1", s"session$i", tracks)
    }.toList
    
    // Act
    val top3 = SessionAnalyzer.selectTopSessions(sessions, 3)
    
    // Assert
    top3 should have length 3
    top3.head.trackCount should be(5)  // Largest session first
    top3.last.trackCount should be(3)  // Third largest session
  }
  
  it should "calculate track popularity correctly" in {
    // Arrange
    val tracks = List(
      createEvent("user1", "2023-01-01T10:00:00Z", "Artist1", "Track1"),
      createEvent("user2", "2023-01-01T10:01:00Z", "Artist1", "Track1"),  // Same track
      createEvent("user3", "2023-01-01T10:02:00Z", "Artist2", "Track2")   // Different track
    )
    
    // Act
    val popularity = SessionAnalyzer.calculateTrackPopularity(tracks)
    
    // Assert
    popularity("Artist1 - Track1") should be(2)
    popularity("Artist2 - Track2") should be(1)
  }
  
  it should "select top tracks by popularity" in {
    // Arrange
    val popularity = Map(
      "Track A" -> 10,
      "Track B" -> 5,
      "Track C" -> 15,
      "Track D" -> 3
    )
    
    // Act
    val top2 = SessionAnalyzer.selectTopTracks(popularity, 2)
    
    // Assert
    top2 should have length 2
    top2.head._1 should be("Track C")  // Most popular
    top2.head._2 should be(15)
    top2.last._1 should be("Track A")  // Second most popular
    top2.last._2 should be(10)
  }
}