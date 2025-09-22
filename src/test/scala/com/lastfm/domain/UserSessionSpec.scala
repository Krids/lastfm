package com.lastfm.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.{Instant, Duration}

/**
 * Tests for the UserSession domain model.
 * 
 * Validates session behavior, duration calculations, and business rules.
 */
class UserSessionSpec extends AnyFlatSpec with Matchers {
  
  private def createEvent(timestamp: String, artist: String, track: String): ListenEvent = {
    ListenEvent("user1", Instant.parse(timestamp), artist, track)
  }
  
  "UserSession" should "calculate track count correctly" in {
    // Arrange
    val tracks = List(
      createEvent("2023-01-01T10:00:00Z", "Artist1", "Track1"),
      createEvent("2023-01-01T10:05:00Z", "Artist2", "Track2")
    )
    
    // Act
    val session = UserSession("user1", "session1", tracks)
    
    // Assert
    session.trackCount should be(2)
  }
  
  it should "identify start and end times correctly" in {
    // Arrange
    val tracks = List(
      createEvent("2023-01-01T10:00:00Z", "Artist1", "Track1"),
      createEvent("2023-01-01T10:30:00Z", "Artist2", "Track2")
    )
    
    // Act
    val session = UserSession("user1", "session1", tracks)
    
    // Assert
    session.startTime should be(Instant.parse("2023-01-01T10:00:00Z"))
    session.endTime should be(Instant.parse("2023-01-01T10:30:00Z"))
  }
  
  it should "calculate duration correctly" in {
    // Arrange
    val tracks = List(
      createEvent("2023-01-01T10:00:00Z", "Artist1", "Track1"),
      createEvent("2023-01-01T10:45:00Z", "Artist2", "Track2")
    )
    
    // Act
    val session = UserSession("user1", "session1", tracks)
    
    // Assert
    session.duration should be(Duration.ofMinutes(45))
    session.durationMinutes should be(45.0)
  }
  
  it should "validate chronological ordering" in {
    // Arrange - Properly ordered tracks
    val orderedTracks = List(
      createEvent("2023-01-01T10:00:00Z", "Artist1", "Track1"),
      createEvent("2023-01-01T10:05:00Z", "Artist2", "Track2"),
      createEvent("2023-01-01T10:10:00Z", "Artist3", "Track3")
    )
    
    // Act
    val session = UserSession("user1", "session1", orderedTracks)
    
    // Assert
    session.isChronologicallyOrdered should be(true)
  }
  
  it should "detect non-chronological ordering" in {
    // Arrange - Out of order tracks
    val unorderedTracks = List(
      createEvent("2023-01-01T10:00:00Z", "Artist1", "Track1"),
      createEvent("2023-01-01T10:10:00Z", "Artist2", "Track2"),
      createEvent("2023-01-01T10:05:00Z", "Artist3", "Track3")  // Out of order
    )
    
    // Act
    val session = UserSession("user1", "session1", unorderedTracks)
    
    // Assert
    session.isChronologicallyOrdered should be(false)
  }
  
  it should "handle single track session duration" in {
    // Arrange
    val tracks = List(createEvent("2023-01-01T10:00:00Z", "Artist1", "Track1"))
    
    // Act
    val session = UserSession("user1", "session1", tracks)
    
    // Assert
    session.duration should be(Duration.ZERO)
    session.durationMinutes should be(0.0)
  }
}