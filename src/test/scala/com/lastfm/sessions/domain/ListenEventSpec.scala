package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Test specification for the ListenEvent domain model.
 * 
 * Each test method validates exactly one aspect of the ListenEvent behavior,
 * following the Single Responsibility Principle to ensure clear failure messages
 * and focused validation.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class ListenEventSpec extends AnyFlatSpec with Matchers {
  
  /**
   * Tests for successful ListenEvent creation scenarios.
   */
  "ListenEvent creation" should "accept valid userId" in {
    // Arrange
    val validUserId = "user_000001"
    
    // Act
    val event = ListenEvent.minimal(
      userId = validUserId,
      timestamp = Instant.now(),
      artistName = "Test Artist",
      trackName = "Test Track"
    )
    
    // Assert - Only test userId assignment
    event.userId should be(validUserId)
  }
  
  it should "accept valid timestamp" in {
    // Arrange
    val validTimestamp = Instant.parse("2009-05-04T23:08:57Z")
    
    // Act
    val event = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = validTimestamp,
      artistName = "Test Artist",
      trackName = "Test Track"
    )
    
    // Assert - Only test timestamp assignment
    event.timestamp should be(validTimestamp)
  }
  
  it should "accept valid artistName" in {
    // Arrange
    val validArtistName = "Deep Dish"
    
    // Act
    val event = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistName = validArtistName,
      trackName = "Test Track"
    )
    
    // Assert - Only test artistName assignment
    event.artistName should be(validArtistName)
  }
  
  it should "accept valid trackName" in {
    // Arrange
    val validTrackName = "Fuck Me Im Famous (Pacha Ibiza)-09-28-2007"
    
    // Act
    val event = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistName = "Test Artist",
      trackName = validTrackName
    )
    
    // Assert - Only test trackName assignment
    event.trackName should be(validTrackName)
  }
  
  it should "accept optional artistId as Some" in {
    // Arrange
    val artistId = Some("f1b1cf71-bd35-4e99-8624-24a6e15f133a")
    
    // Act - Use full constructor to test optional artistId
    val event = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistId = artistId,
      artistName = "Test Artist",
      trackId = None,
      trackName = "Test Track",
      trackKey = "Test Artist — Test Track" // Fallback since no trackId
    )
    
    // Assert - Only test artistId assignment
    event.artistId should be(artistId)
  }
  
  it should "accept optional artistId as None" in {
    // Act
    val event = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistName = "Test Artist",
      trackName = "Test Track"
    )
    
    // Assert - Only test artistId None handling
    event.artistId should be(None)
  }
  
  it should "accept optional trackId as Some" in {
    // Arrange
    val trackId = Some("track_123")
    
    // Act - Use full constructor to test optional trackId
    val event = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistId = None,
      artistName = "Test Artist",
      trackId = trackId,
      trackName = "Test Track",
      trackKey = "track_123" // Use trackId as key when available
    )
    
    // Assert - Only test trackId assignment
    event.trackId should be(trackId)
  }
  
  it should "accept optional trackId as None" in {
    // Act
    val event = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistName = "Test Artist",
      trackName = "Test Track"
    )
    
    // Assert - Only test trackId None handling
    event.trackId should be(None)
  }
  
  /**
   * Tests for validation rules - each test validates one specific constraint.
   */
  "ListenEvent validation" should "reject null userId" in {
    // Act & Assert - Only test null userId rejection
    an [IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = null,
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Test Artist",
        trackId = None,
        trackName = "Test Track",
        trackKey = "Test Artist — Test Track"
      )
    }
  }
  
  it should "reject null timestamp" in {
    // Act & Assert - Only test null timestamp rejection
    an [IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = null,
        artistId = None,
        artistName = "Test Artist",
        trackId = None,
        trackName = "Test Track",
        trackKey = "Test Artist — Test Track"
      )
    }
  }
  
  it should "reject empty artistName" in {
    // Act & Assert - Only test empty artistName rejection
    an [IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "",
        trackId = None,
        trackName = "Test Track",
        trackKey = "Test Track" // Will fail anyway due to empty artistName
      )
    }
  }
  
  it should "reject blank artistName" in {
    // Act & Assert - Only test blank artistName rejection  
    an [IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "   ",
        trackId = None,
        trackName = "Test Track",
        trackKey = "Test Track" // Will fail anyway due to blank artistName
      )
    }
  }
  
  it should "reject null artistName" in {
    // Act & Assert - Only test null artistName rejection
    an [IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = null,
        trackId = None,
        trackName = "Test Track",
        trackKey = "Test Track" // Will fail anyway due to null artistName
      )
    }
  }
  
  it should "reject empty trackName" in {
    // Act & Assert - Only test empty trackName rejection
    an [IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Test Artist",
        trackId = None,
        trackName = "",
        trackKey = "Test Artist" // Will fail anyway due to empty trackName
      )
    }
  }
  
  it should "reject blank trackName" in {
    // Act & Assert - Only test blank trackName rejection
    an [IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Test Artist",
        trackId = None,
        trackName = "   ",
        trackKey = "Test Artist" // Will fail anyway due to blank trackName
      )
    }
  }
  
  it should "reject null trackName" in {
    // Act & Assert - Only test null trackName rejection
    an [IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Test Artist",
        trackId = None,
        trackName = null,
        trackKey = "Test Artist" // Will fail anyway due to null trackName
      )
    }
  }
  
  /**
   * Tests for special character handling - each test focuses on one type of special character.
   */
  "ListenEvent internationalization" should "handle Japanese characters in artistName" in {
    // Arrange
    val japaneseArtist = "坂本龍一"
    
    // Act
    val event = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistName = japaneseArtist,
      trackName = "Test Track"
    )
    
    // Assert - Only test Japanese character preservation
    event.artistName should be(japaneseArtist)
  }
  
  it should "handle Unicode characters in trackName" in {
    // Arrange
    val unicodeTrack = "Composition 0919 (Live_2009_4_15)"
    
    // Act
    val event = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistName = "Test Artist",
      trackName = unicodeTrack
    )
    
    // Assert - Only test Unicode character preservation
    event.trackName should be(unicodeTrack)
  }
  
  it should "handle special characters in artistName" in {
    // Arrange
    val specialCharArtist = "Sigur Rós"
    
    // Act
    val event = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistName = specialCharArtist,
      trackName = "Test Track"
    )
    
    // Assert - Only test special character preservation
    event.artistName should be(specialCharArtist)
  }
  
  /**
   * Tests for the companion object factory methods.
   */
  "ListenEvent.minimal factory" should "create event with minimal required fields" in {
    // Arrange
    val userId = "user_000001"
    val timestamp = Instant.now()
    val artistName = "Test Artist"
    val trackName = "Test Track"
    
    // Act
    val event = ListenEvent.minimal(userId, timestamp, artistName, trackName)
    
    // Assert - Only test factory method creation
    event.userId should be(userId)
    event.timestamp should be(timestamp)
    event.artistName should be(artistName)
    event.trackName should be(trackName)
    event.artistId should be(None)
    event.trackId should be(None)
  }
}