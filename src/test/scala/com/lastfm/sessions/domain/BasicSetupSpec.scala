package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Basic setup verification tests to ensure everything is working.
 * 
 * These simple tests verify that:
 * 1. ScalaTest is working correctly
 * 2. Domain classes can be instantiated
 * 3. Test runner can find and execute tests
 */
class BasicSetupSpec extends AnyFlatSpec with Matchers {
  
  "Setup verification" should "work correctly" in {
    val result = 1 + 1
    result should be(2)
  }
  
  it should "handle strings" in {
    val text = "Hello Scala"
    text should include("Scala")
  }
  
  "ListenEvent" should "create minimal events" in {
    val event = ListenEvent.minimal(
      "user_001", 
      Instant.now(),
      "Test Artist",
      "Test Track"
    )
    
    event.userId should be("user_001")
    event.artistName should be("Test Artist")
    event.artistId should be(None)
    event.trackId should be(None)
  }
  
  it should "create full events" in {
    val timestamp = Instant.parse("2009-05-04T23:08:57Z")
    val event = ListenEvent(
      userId = "user_000001",
      timestamp = timestamp,
      artistId = Some("artist-123"),
      artistName = "Deep Dish",
      trackId = Some("track-456"),
      trackName = "Fuck Me Im Famous"
    )
    
    event.userId should be("user_000001")
    event.timestamp should be(timestamp)
    event.artistId should be(Some("artist-123"))
    event.trackId should be(Some("track-456"))
  }
  
  "Collections" should "work properly" in {
    val events = List(
      ListenEvent.minimal("user_001", Instant.now(), "Artist1", "Track1"),
      ListenEvent.minimal("user_002", Instant.now(), "Artist2", "Track2")
    )
    
    events should have size 2
    events.map(_.userId) should contain allOf("user_001", "user_002")
  }
}



