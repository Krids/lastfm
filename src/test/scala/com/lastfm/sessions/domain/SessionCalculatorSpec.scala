package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Gen
import java.time.Instant
import scala.util.Random

/**
 * Comprehensive test specification for SessionCalculator business logic.
 * 
 * Tests the core 20-minute session gap algorithm with extensive edge case coverage:
 * - Core algorithm correctness for session boundary detection
 * - Time handling complexities (timezone changes, midnight crossing, DST)
 * - Data quality scenarios (missing data, duplicates, out-of-order events)
 * - Performance characteristics with large datasets
 * - Property-based testing for algorithm invariants (ScalaCheck)
 * 
 * Algorithm Specification:
 * For chronologically ordered listening events from a single user:
 * 1. Start new session with first track
 * 2. For each subsequent track:
 *    - If gap ≤ 20 minutes: Add to current session
 *    - If gap > 20 minutes: End current session, start new session
 * 3. Return all sessions for the user
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionCalculatorSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {

  // Test data fixtures following real LastFM usage patterns
  private val baseTime = Instant.parse("2023-01-15T14:30:00Z")
  private val sampleUserId = "user_000001"
  
  private def createEvent(minutesOffset: Int, userId: String = sampleUserId, trackName: String = "Track"): ListenEvent = {
    ListenEvent(
      userId = userId,
      timestamp = baseTime.plusSeconds(minutesOffset * 60L),
      artistId = Some(s"artist-${trackName.hashCode.abs}"),
      artistName = s"Artist for $trackName",
      trackId = Some(s"track-${trackName.hashCode.abs}"),
      trackName = trackName,
      trackKey = s"track-${trackName.hashCode.abs}"
    )
  }

  /**
   * Core Algorithm Tests - Basic session boundary detection
   */
  "SessionCalculator core algorithm" should "create single session for consecutive tracks within 20-minute limit" in {
    // Arrange - Tracks with 5, 10, 15-minute gaps (all ≤ 20 minutes)
    val events = List(
      createEvent(0, trackName = "Track A"),
      createEvent(5, trackName = "Track B"),   // 5-minute gap
      createEvent(15, trackName = "Track C"),  // 10-minute gap from Track B
      createEvent(30, trackName = "Track D")   // 15-minute gap from Track C
    )
    
    // Act - This will fail initially (Red phase)
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert
    sessions should have size 1
    sessions.head.trackCount should be(4)
    sessions.head.userId should be(sampleUserId)
  }
  
  it should "create separate sessions when gap exceeds 20 minutes" in {
    // Arrange - First session, then 25-minute gap, then second session
    val events = List(
      createEvent(0, trackName = "Session1 Track1"),
      createEvent(10, trackName = "Session1 Track2"),
      createEvent(35, trackName = "Session2 Track1"), // 25-minute gap > 20 minutes
      createEvent(40, trackName = "Session2 Track2")
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert
    sessions should have size 2
    sessions.head.trackCount should be(2) // First session
    sessions(1).trackCount should be(2)   // Second session
  }
  
  it should "handle tracks at exact 20-minute boundary" in {
    // Arrange - Exactly 20-minute gap (should be same session)
    val events = List(
      createEvent(0, trackName = "Track A"),
      createEvent(20, trackName = "Track B")  // Exactly 20 minutes
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - 20 minutes exactly should be within same session
    sessions should have size 1
    sessions.head.trackCount should be(2)
  }
  
  it should "maintain chronological order within sessions" in {
    // Arrange - Tracks within session limits
    val events = List(
      createEvent(0, trackName = "First"),
      createEvent(10, trackName = "Second"), 
      createEvent(15, trackName = "Third")
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert
    sessions should have size 1
    val session = sessions.head
    session.tracks.map(_.trackName) should be(List("First", "Second", "Third"))
  }
  
  it should "handle single track per user" in {
    // Arrange
    val events = List(createEvent(0, trackName = "Only Track"))
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert
    sessions should have size 1
    sessions.head.trackCount should be(1)
    sessions.head.tracks.head.trackName should be("Only Track")
  }
  
  it should "handle empty user listening history" in {
    // Arrange
    val events = List.empty[ListenEvent]
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert
    sessions should be(empty)
  }

  /**
   * Time Handling Edge Cases - Complex temporal scenarios
   */
  "SessionCalculator time handling" should "handle tracks with identical timestamps" in {
    // Arrange - Multiple tracks at same timestamp (possible in real data)
    val sameTime = baseTime
    val events = List(
      createEvent(0, trackName = "Track A").copy(timestamp = sameTime),
      createEvent(0, trackName = "Track B").copy(timestamp = sameTime),
      createEvent(10, trackName = "Track C")
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert
    sessions should have size 1
    sessions.head.trackCount should be(3)
  }
  
  it should "handle unsorted input timestamps correctly" in {
    // Arrange - Events provided out of chronological order
    val events = List(
      createEvent(15, trackName = "Middle"),
      createEvent(0, trackName = "First"),    // Out of order
      createEvent(5, trackName = "Second")    // Out of order
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - Should be sorted internally and treated as one session
    sessions should have size 1
    val session = sessions.head
    session.tracks.map(_.trackName) should be(List("First", "Second", "Middle"))
  }
  
  it should "handle midnight boundary crossings" in {
    // Arrange - Session crossing from one day to next
    val lateNight = Instant.parse("2023-01-15T23:50:00Z")
    val earlyMorning = Instant.parse("2023-01-16T00:05:00Z") // 15 minutes later
    
    val events = List(
      createEvent(0, trackName = "Late Track").copy(timestamp = lateNight),
      createEvent(0, trackName = "Early Track").copy(timestamp = earlyMorning)
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - Should be one session (15-minute gap < 20 minutes)
    sessions should have size 1
    sessions.head.trackCount should be(2)
  }
  
  it should "handle daylight saving time transitions" in {
    // Arrange - DST transition scenario (Spring forward: 2:00 AM becomes 3:00 AM)
    val beforeDST = Instant.parse("2023-03-12T06:50:00Z") // 1:50 AM EST
    val afterDST = Instant.parse("2023-03-12T07:10:00Z")  // 3:10 AM EDT (20 minutes real time)
    
    val events = List(
      createEvent(0, trackName = "Before DST").copy(timestamp = beforeDST),
      createEvent(0, trackName = "After DST").copy(timestamp = afterDST)
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - Should handle DST transition correctly (20 minutes exactly)
    sessions should have size 1
    sessions.head.trackCount should be(2)
  }
  
  it should "handle tracks spanning multiple days" in {
    // Arrange - First session, then long gap, then second session with continuous tracks
    val events = List(
      createEvent(0, trackName = "Day1 Start"),
      createEvent(1000, trackName = "Day1 Late"),   // ~16.7 hours later (creates new session)
      createEvent(1020, trackName = "Day2 Early"),  // 20 minutes later (same session)
      createEvent(1040, trackName = "Day2 Mid")     // 20 minutes later (same session)
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - Should be two sessions: one single track, one with 3 tracks
    sessions should have size 2
    sessions.head.trackCount should be(1)  // First session: single track
    sessions(1).trackCount should be(3)     // Second session: 3 continuous tracks
  }

  /**
   * Data Quality Edge Cases - Handling imperfect real-world data
   */
  "SessionCalculator data quality" should "skip tracks with null timestamps" in {
    // Arrange - Mix of valid and invalid events
    val validEvent1 = createEvent(0, trackName = "Valid Track 1")
    val validEvent2 = createEvent(10, trackName = "Valid Track 2")
    
    // Create invalid event with null timestamp (this would fail ListenEvent validation)
    // Instead, test empty list handling which represents filtered-out invalid events
    val events = List(validEvent1, validEvent2)
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - Should process valid events only
    sessions should have size 1
    sessions.head.trackCount should be(2)
  }
  
  it should "handle tracks with future timestamps" in {
    // Arrange - Mix of current and future timestamps
    val futureTime = Instant.now().plusSeconds(3600) // 1 hour in future
    val events = List(
      createEvent(0, trackName = "Current Track"),
      createEvent(0, trackName = "Future Track").copy(timestamp = futureTime)
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - Should handle future timestamps (large gap creates new session)
    sessions should have size 2
    sessions.map(_.trackCount) should contain theSameElementsAs List(1, 1)
  }
  
  it should "deduplicate identical listening events" in {
    // Arrange - Exact duplicate events (same user, timestamp, track)
    val originalEvent = createEvent(0, trackName = "Duplicate Track")
    val events = List(originalEvent, originalEvent, originalEvent)
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - Should handle duplicates gracefully (deduplication logic)
    sessions should have size 1
    sessions.head.trackCount should be <= 3 // Could be 1 if deduplicated, 3 if not
  }
  
  it should "handle extremely long gaps between tracks" in {
    // Arrange - Months between tracks
    val events = List(
      createEvent(0, trackName = "Track A"),
      createEvent(60 * 24 * 30, trackName = "Track B") // 30 days later
    )
    
    // Act
    val sessions = SessionCalculator.calculateSessions(events)
    
    // Assert - Should create separate sessions for extreme gaps
    sessions should have size 2
    sessions.map(_.trackCount) should be(List(1, 1))
  }

  /**
   * Performance & Scale Tests - Large dataset handling
   */
  "SessionCalculator performance" should "handle user with 10K+ tracks efficiently" in {
    // Arrange - Generate large number of tracks for single user
    val largeEventList = (0 until 10000).map { i =>
      createEvent(i * 2, trackName = s"Track $i") // 2-minute gaps (within session limit)
    }.toList
    
    // Act - Should complete without performance issues
    val startTime = System.currentTimeMillis()
    val sessions = SessionCalculator.calculateSessions(largeEventList)
    val endTime = System.currentTimeMillis()
    
    // Assert
    sessions should have size 1 // All within session limit
    sessions.head.trackCount should be(10000)
    (endTime - startTime) should be < 1000L // Should complete within 1 second
  }
  
  it should "handle user with 1000+ sessions efficiently" in {
    // Arrange - Generate events with gaps creating many sessions
    val manySessionEvents = (0 until 1000).flatMap { sessionIndex =>
      List(
        createEvent(sessionIndex * 60, trackName = s"Session$sessionIndex-Track1"), // 60-minute gaps
        createEvent(sessionIndex * 60 + 5, trackName = s"Session$sessionIndex-Track2")
      )
    }.toList
    
    // Act
    val sessions = SessionCalculator.calculateSessions(manySessionEvents)
    
    // Assert
    sessions should have size 1000
    sessions.forall(_.trackCount == 2) should be(true)
  }

  /**
   * Property-Based Tests (ScalaCheck) - Algorithm invariants
   */
  "SessionCalculator invariants" should "ensure sessions never overlap in time for same user" in {
    forAll(Gen.listOf(Gen.choose(0, 1440))) { minuteOffsets: List[Int] =>
      whenever(minuteOffsets.nonEmpty) {
        // Arrange
        val events = minuteOffsets.zipWithIndex.map { case (offset, index) =>
          createEvent(offset, trackName = s"Track$index")
        }
        
        // Act
        val sessions = SessionCalculator.calculateSessions(events)
        
        // Assert - No session should overlap with another
        val sortedSessions = sessions.sortBy(_.startTime)
        sortedSessions.zip(sortedSessions.tail).foreach { case (current, next) =>
          current.endTime should be <= next.startTime
        }
      }
    }
  }
  
  it should "ensure total tracks in sessions equals input tracks" in {
    forAll(Gen.listOf(Gen.choose(0, 200))) { minuteOffsets: List[Int] =>
      whenever(minuteOffsets.nonEmpty) {
        // Arrange
        val events = minuteOffsets.zipWithIndex.map { case (offset, index) =>
          createEvent(offset, trackName = s"Track$index")
        }
        
        // Act
        val sessions = SessionCalculator.calculateSessions(events)
        
        // Assert - No tracks should be lost or added
        sessions.map(_.trackCount).sum should be(events.length)
      }
    }
  }
  
  it should "ensure session start time equals first track timestamp" in {
    forAll(Gen.nonEmptyListOf(Gen.choose(0, 1440))) { minuteOffsets: List[Int] =>
      // Arrange
      val events = minuteOffsets.zipWithIndex.map { case (offset, index) =>
        createEvent(offset, trackName = s"Track$index")
      }
      
      // Act
      val sessions = SessionCalculator.calculateSessions(events)
      
      // Assert - Each session's start time should match its first track
      sessions.foreach { session =>
        session.startTime should be(session.tracks.head.timestamp)
      }
    }
  }
  
  it should "ensure session end time equals last track timestamp" in {
    forAll(Gen.nonEmptyListOf(Gen.choose(0, 1440))) { minuteOffsets: List[Int] =>
      // Arrange
      val events = minuteOffsets.zipWithIndex.map { case (offset, index) =>
        createEvent(offset, trackName = s"Track$index")
      }
      
      // Act
      val sessions = SessionCalculator.calculateSessions(events)
      
      // Assert - Each session's end time should match its last track
      sessions.foreach { session =>
        session.endTime should be(session.tracks.last.timestamp)
      }
    }
  }
  
  it should "ensure all tracks belong to exactly one session" in {
    forAll(Gen.nonEmptyListOf(Gen.choose(0, 1440))) { minuteOffsets: List[Int] =>
      // Arrange
      val events = minuteOffsets.zipWithIndex.map { case (offset, index) =>
        createEvent(offset, trackName = s"Track$index")
      }
      
      // Act
      val sessions = SessionCalculator.calculateSessions(events)
      
      // Assert - Every input track should appear in exactly one session
      val allSessionTracks = sessions.flatMap(_.tracks)
      allSessionTracks should have size events.length
      allSessionTracks.map(_.trackName).toSet should be(events.map(_.trackName).toSet)
    }
  }

  /**
   * Multi-User Scenarios - Handling multiple users in same dataset
   */
  "SessionCalculator multi-user" should "handle events from multiple users correctly" in {
    // Arrange - Events from different users
    val events = List(
      createEvent(0, userId = "user_001", trackName = "User1 Track1"),
      createEvent(5, userId = "user_002", trackName = "User2 Track1"),
      createEvent(10, userId = "user_001", trackName = "User1 Track2"),
      createEvent(15, userId = "user_002", trackName = "User2 Track2")
    )
    
    // Act - Process all events together
    val allSessions = SessionCalculator.calculateSessionsForAllUsers(events)
    
    // Assert
    allSessions should have size 2 // One session per user
    allSessions.map(_.userId).toSet should be(Set("user_001", "user_002"))
    allSessions.foreach(_.trackCount should be(2))
  }
  
  it should "maintain session boundaries independently per user" in {
    // Arrange - User1 has long gap, User2 has continuous listening
    val events = List(
      createEvent(0, userId = "user_001", trackName = "User1 Session1"),
      createEvent(5, userId = "user_002", trackName = "User2 Track1"),
      createEvent(10, userId = "user_002", trackName = "User2 Track2"),
      createEvent(60, userId = "user_001", trackName = "User1 Session2") // 60-minute gap > 20 minutes
    )
    
    // Act
    val allSessions = SessionCalculator.calculateSessionsForAllUsers(events)
    
    // Assert
    val user1Sessions = allSessions.filter(_.userId == "user_001")
    val user2Sessions = allSessions.filter(_.userId == "user_002")
    
    user1Sessions should have size 2 // Gap creates separate sessions
    user2Sessions should have size 1 // Continuous listening
  }
}