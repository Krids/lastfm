package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Test specification for UserSession domain model.
 * 
 * Tests follow TDD approach with comprehensive edge case coverage:
 * - Happy path scenarios for valid session creation
 * - Input validation for required fields
 * - Business logic verification for session calculations
 * - Edge cases for temporal boundaries and data quality
 * 
 * Follows clean code principles with expressive test names that
 * describe expected behavior clearly.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class UserSessionSpec extends AnyFlatSpec with Matchers {

  // Test data fixtures following real LastFM patterns
  private val sampleUserId = "user_000001"
  private val baseTime = Instant.parse("2023-01-15T14:30:00Z")
  
  private def createTrack(minutesOffset: Int, trackName: String = "Sample Track", artistName: String = "Sample Artist"): ListenEvent = {
    // Generate unique trackKey based on track name for testing unique counting
    val trackId = s"mbid-track-${trackName.hashCode.abs}"
    
    ListenEvent(
      userId = sampleUserId,
      timestamp = baseTime.plusSeconds(minutesOffset * 60L),
      artistId = Some("mbid-artist-123"),
      artistName = artistName,
      trackId = Some(trackId),
      trackName = trackName,
      trackKey = trackId
    )
  }

  /**
   * Happy Path Tests - Core functionality with valid inputs
   */
  "UserSession creation" should "create session with valid userId and tracks" in {
    // Arrange
    val tracks = List(
      createTrack(0, "First Track"),
      createTrack(5, "Second Track"), 
      createTrack(10, "Third Track")
    )
    
    // Act & Assert - This should fail initially (Red phase)
    val session = UserSession(sampleUserId, tracks)
    
    session.userId should be(sampleUserId)
    session.tracks should be(tracks)
    session.trackCount should be(3)
  }
  
  it should "calculate session duration correctly" in {
    // Arrange - 15-minute session
    val tracks = List(
      createTrack(0, "First Track"),
      createTrack(15, "Last Track")
    )
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert
    session.duration.toMinutes should be(15)
  }
  
  it should "maintain chronological track ordering" in {
    // Arrange - Tracks in chronological order
    val tracks = List(
      createTrack(0, "First"),
      createTrack(5, "Second"),
      createTrack(10, "Third")
    )
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert - Tracks should remain in order
    session.tracks.map(_.trackName) should be(List("First", "Second", "Third"))
  }
  
  it should "handle session with single track" in {
    // Arrange
    val tracks = List(createTrack(0, "Only Track"))
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert
    session.trackCount should be(1)
    session.duration.toSeconds should be(0) // Single track = zero duration
  }
  
  it should "provide session start and end times" in {
    // Arrange
    val tracks = List(
      createTrack(0, "First"),
      createTrack(10, "Last")
    )
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert
    session.startTime should be(baseTime)
    session.endTime should be(baseTime.plusSeconds(10 * 60L))
  }

  /**
   * Validation Tests - Input validation and error cases
   */
  "UserSession validation" should "reject null userId" in {
    // Arrange
    val tracks = List(createTrack(0))
    
    // Act & Assert
    intercept[IllegalArgumentException] {
      UserSession(null, tracks)
    }.getMessage should include("userId cannot be null")
  }
  
  it should "reject empty userId" in {
    // Arrange
    val tracks = List(createTrack(0))
    
    // Act & Assert
    intercept[IllegalArgumentException] {
      UserSession("", tracks)
    }.getMessage should include("userId cannot be empty")
  }
  
  it should "reject blank userId" in {
    // Arrange
    val tracks = List(createTrack(0))
    
    // Act & Assert
    intercept[IllegalArgumentException] {
      UserSession("   ", tracks)
    }.getMessage should include("userId cannot be blank")
  }
  
  it should "reject null tracks list" in {
    // Act & Assert
    intercept[IllegalArgumentException] {
      UserSession(sampleUserId, null)
    }.getMessage should include("tracks cannot be null")
  }
  
  it should "reject empty tracks list" in {
    // Act & Assert
    intercept[IllegalArgumentException] {
      UserSession(sampleUserId, List.empty)
    }.getMessage should include("tracks cannot be empty")
  }

  /**
   * Edge Cases - Complex scenarios and boundary conditions
   */
  "UserSession edge cases" should "handle session spanning midnight" in {
    // Arrange - Session from 23:30 to 00:30 next day
    val lateNight = Instant.parse("2023-01-15T23:30:00Z")
    val afterMidnight = Instant.parse("2023-01-16T00:30:00Z")
    
    val tracks = List(
      createTrack(0).copy(timestamp = lateNight),
      createTrack(0).copy(timestamp = afterMidnight)
    )
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert - Should handle 60-minute duration correctly
    session.duration.toMinutes should be(60)
  }
  
  it should "handle tracks with identical timestamps" in {
    // Arrange - Multiple tracks at same timestamp
    val sameTime = baseTime
    val tracks = List(
      createTrack(0).copy(timestamp = sameTime, trackName = "Track A"),
      createTrack(0).copy(timestamp = sameTime, trackName = "Track B")
    )
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert
    session.trackCount should be(2)
    session.duration.toSeconds should be(0) // Same timestamp = zero duration
  }
  
  it should "handle extremely long sessions" in {
    // Arrange - Create session with 100 tracks over several hours
    val tracks = (0 until 100).map(i => createTrack(i * 2, s"Track $i")).toList
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert
    session.trackCount should be(100)
    session.duration.toMinutes should be(198) // 99 * 2 minutes
  }

  /**
   * Business Logic Tests - Calculations and derived properties
   */
  "UserSession business logic" should "calculate unique track count correctly" in {
    // Arrange - Some duplicate tracks in session
    val tracks = List(
      createTrack(0, "Song A"),
      createTrack(5, "Song B"),
      createTrack(10, "Song A"), // Duplicate
      createTrack(15, "Song C")
    )
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert
    session.trackCount should be(4) // Total plays
    session.uniqueTrackCount should be(3) // Unique tracks
  }
  
  it should "handle tracks played multiple times in same session" in {
    // Arrange - Same song played 3 times
    val sameSong = "Favorite Song"
    val tracks = List(
      createTrack(0, sameSong),
      createTrack(10, sameSong),
      createTrack(20, sameSong)
    )
    
    // Act
    val session = UserSession(sampleUserId, tracks)
    
    // Assert
    session.trackCount should be(3)
    session.uniqueTrackCount should be(1)
    session.tracks.forall(_.trackName == sameSong) should be(true)
  }
  
  it should "maintain immutability after creation" in {
    // Arrange
    val originalTracks = List(createTrack(0), createTrack(5))
    val session = UserSession(sampleUserId, originalTracks)
    
    // Act - Try to modify the session's track list
    val sessionTracks = session.tracks
    
    // Assert - Should get a copy, not the original mutable list
    sessionTracks should be(originalTracks)
    // Verify immutability by ensuring we can't modify the returned list
    sessionTracks shouldBe a[List[_]]
  }
}

