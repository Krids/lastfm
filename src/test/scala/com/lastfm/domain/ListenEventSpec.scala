package com.lastfm.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Tests for the ListenEvent domain model.
 * 
 * Focuses on essential validation and behavior of the core data model.
 * Demonstrates clean testing practices for domain objects.
 */
class ListenEventSpec extends AnyFlatSpec with Matchers {
  
  "ListenEvent" should "create valid event with required fields" in {
    // Arrange
    val userId = "user_001"
    val timestamp = Instant.parse("2023-01-01T10:00:00Z")
    val artistName = "Test Artist"
    val trackName = "Test Track"
    
    // Act
    val event = ListenEvent(userId, timestamp, artistName, trackName)
    
    // Assert
    event.userId should be(userId)
    event.timestamp should be(timestamp)
    event.artistName should be(artistName)
    event.trackName should be(trackName)
    event.trackKey should be("Test Artist - Test Track")
  }
  
  it should "reject empty userId" in {
    assertThrows[IllegalArgumentException] {
      ListenEvent("", Instant.now(), "Artist", "Track")
    }
  }
  
  it should "reject empty artistName" in {
    assertThrows[IllegalArgumentException] {
      ListenEvent("user1", Instant.now(), "", "Track")
    }
  }
  
  it should "reject empty trackName" in {
    assertThrows[IllegalArgumentException] {
      ListenEvent("user1", Instant.now(), "Artist", "")
    }
  }
  
  it should "generate correct trackKey for identity resolution" in {
    val event = ListenEvent("user1", Instant.now(), "The Beatles", "Hey Jude")
    event.trackKey should be("The Beatles - Hey Jude")
  }
}
