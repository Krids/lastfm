package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import java.time.{Duration, Instant}

/**
 * TDD-First specification for SessionBoundaryDetector domain logic.
 * 
 * Tests the core session boundary detection algorithm using the 20-minute gap rule.
 * Uses property-based testing for comprehensive coverage of edge cases.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionBoundaryDetectorSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  // Test data factory methods
  private def createEvent(userId: String, timestamp: String): ListenEvent = {
    ListenEvent(
      userId = userId,
      timestamp = Instant.parse(s"${timestamp}Z"),
      artistId = Some("artist1"),
      artistName = "Test Artist",
      trackId = Some("track1"),
      trackName = "Test Track",
      trackKey = "Test Artist — Test Track"
    )
  }

  "SessionBoundaryDetector" should "identify session boundaries with 20-minute gaps" in {
    // Given: Events with various time gaps
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00"),
      createEvent("user1", "2023-01-01T10:15:00"), // Same session (15 min gap)
      createEvent("user1", "2023-01-01T10:45:00")  // New session (30 min gap from previous)
    )
    
    // When: Boundaries are detected with 20-minute session gap
    val boundaries = SessionBoundaryDetector.detectBoundaries(events, Duration.ofMinutes(20))
    
    // Then: Correct boundaries are identified
    boundaries should contain theSameElementsAs List(0, 2) // First event and third event start new sessions
  }
  
  it should "handle single event correctly" in {
    // Given: Single event
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00")
    )
    
    // When: Boundaries are detected
    val boundaries = SessionBoundaryDetector.detectBoundaries(events, Duration.ofMinutes(20))
    
    // Then: Single boundary at first event
    boundaries shouldBe List(0)
  }
  
  it should "handle empty event list" in {
    // Given: Empty event list
    val events = List.empty[ListenEvent]
    
    // When: Boundaries are detected
    val boundaries = SessionBoundaryDetector.detectBoundaries(events, Duration.ofMinutes(20))
    
    // Then: No boundaries
    boundaries shouldBe empty
  }
  
  it should "treat all events as same session when gaps are within threshold" in {
    // Given: Events all within 20-minute gaps
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00"),
      createEvent("user1", "2023-01-01T10:15:00"), // 15 min gap
      createEvent("user1", "2023-01-01T10:30:00"), // 15 min gap
      createEvent("user1", "2023-01-01T10:45:00")  // 15 min gap
    )
    
    // When: Boundaries are detected
    val boundaries = SessionBoundaryDetector.detectBoundaries(events, Duration.ofMinutes(20))
    
    // Then: Only first event starts a session
    boundaries shouldBe List(0)
  }
  
  it should "create new session for each event when all gaps exceed threshold" in {
    // Given: Events with gaps all exceeding 20 minutes
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00"),
      createEvent("user1", "2023-01-01T10:30:00"), // 30 min gap
      createEvent("user1", "2023-01-01T11:00:00"), // 30 min gap
      createEvent("user1", "2023-01-01T11:30:00")  // 30 min gap
    )
    
    // When: Boundaries are detected
    val boundaries = SessionBoundaryDetector.detectBoundaries(events, Duration.ofMinutes(20))
    
    // Then: Each event starts a new session
    boundaries shouldBe List(0, 1, 2, 3)
  }
  
  it should "handle events with identical timestamps" in {
    // Given: Events with identical timestamps
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00"),
      createEvent("user1", "2023-01-01T10:00:00"), // Same timestamp
      createEvent("user1", "2023-01-01T10:00:00")  // Same timestamp
    )
    
    // When: Boundaries are detected
    val boundaries = SessionBoundaryDetector.detectBoundaries(events, Duration.ofMinutes(20))
    
    // Then: Only first event starts a session (0 gap = same session)
    boundaries shouldBe List(0)
  }
  
  it should "handle mixed users correctly" in {
    // Given: Events from different users (should be processed separately)
    val events = List(
      createEvent("user1", "2023-01-01T10:00:00"),
      createEvent("user2", "2023-01-01T10:00:00"), // Different user, same time
      createEvent("user1", "2023-01-01T10:15:00")  // user1 continues session
    )
    
    // When: Boundaries are detected (this method processes all events together)
    val boundaries = SessionBoundaryDetector.detectBoundaries(events, Duration.ofMinutes(20))
    
    // Then: Should detect boundaries based on chronological order only
    // Note: This method doesn't group by user - that's handled at higher level
    boundaries should contain(0) // First event always starts a session
  }
  
  "SessionBoundaryDetector with various gap thresholds" should "work with different session gap durations" in {
    val testCases = Table(
      ("description", "events", "sessionGap", "expectedBoundaries"),
      (
        "10-minute gap threshold",
        List(
          createEvent("user1", "2023-01-01T10:00:00"),
          createEvent("user1", "2023-01-01T10:15:00") // 15 min gap > 10 min threshold
        ),
        Duration.ofMinutes(10),
        List(0, 1)
      ),
      (
        "30-minute gap threshold",
        List(
          createEvent("user1", "2023-01-01T10:00:00"),
          createEvent("user1", "2023-01-01T10:15:00") // 15 min gap < 30 min threshold
        ),
        Duration.ofMinutes(30),
        List(0)
      ),
      (
        "1-minute gap threshold",
        List(
          createEvent("user1", "2023-01-01T10:00:00"),
          createEvent("user1", "2023-01-01T10:01:30") // 1.5 min gap > 1 min threshold
        ),
        Duration.ofMinutes(1),
        List(0, 1)
      )
    )
    
    forAll(testCases) { (description, events, sessionGap, expectedBoundaries) =>
      withClue(s"Test case: $description") {
        val boundaries = SessionBoundaryDetector.detectBoundaries(events, sessionGap)
        boundaries shouldBe expectedBoundaries
      }
    }
  }
  
  it should "handle events not in chronological order" in {
    // Given: Events not in chronological order
    val events = List(
      createEvent("user1", "2023-01-01T10:30:00"), // Third chronologically
      createEvent("user1", "2023-01-01T10:00:00"), // First chronologically  
      createEvent("user1", "2023-01-01T10:15:00")  // Second chronologically
    )
    
    // When: Boundaries are detected
    val boundaries = SessionBoundaryDetector.detectBoundaries(events, Duration.ofMinutes(20))
    
    // Then: Should process events in given order, not chronological order
    // (Sorting is responsibility of higher-level components)
    boundaries should contain(0) // First event in list always starts a session
  }
}
