package com.lastfm.sessions.domain.validation

import com.lastfm.sessions.domain.{RankedSession, Track}
import com.lastfm.sessions.utils.BaseTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Comprehensive validation test specification for RankedSession domain model.
 * 
 * Tests all constructor validation rules to achieve complete branch coverage
 * of the RankedSession class validation logic.
 * 
 * Coverage Target: All require statements + business logic branches
 * Expected Impact: +3% statement coverage, +3% branch coverage
 * 
 * Focuses on session ranking scenarios and track count relationships
 * critical for the ranking pipeline functionality.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class RankedSessionValidationSpec extends AnyFlatSpec with BaseTestSpec with Matchers {

  // Helper method to create valid track for testing
  private def createValidTrack(name: String = "Test Track"): Track = {
    Track(
      trackId = "track-id-123",
      trackName = name,
      artistId = "artist-id-456", 
      artistName = "Test Artist"
    )
  }

  /**
   * Tests for constructor validation rules.
   */
  "RankedSession constructor validation" should "reject zero or negative rank" in {
    // Arrange
    val validTracks = List(createValidTrack())
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test require(rank > 0)
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 0,
        sessionId = "session_001",
        userId = "user_000001",
        trackCount = 1,
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = validTracks
      )
    }

    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = -1,
        sessionId = "session_001",
        userId = "user_000001",
        trackCount = 1,
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = validTracks
      )
    }
  }

  it should "reject null session ID" in {
    // Arrange
    val validTracks = List(createValidTrack())
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test require(sessionId != null && sessionId.trim.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = null,
        userId = "user_000001",
        trackCount = 1,
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = validTracks
      )
    }
  }

  it should "reject empty session ID" in {
    // Arrange
    val validTracks = List(createValidTrack())
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test sessionId.trim.nonEmpty
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = "",
        userId = "user_000001",
        trackCount = 1,
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = validTracks
      )
    }
  }

  it should "reject null user ID" in {
    // Arrange
    val validTracks = List(createValidTrack())
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test require(userId != null && userId.trim.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = "session_001",
        userId = null,
        trackCount = 1,
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = validTracks
      )
    }
  }

  it should "reject negative track count" in {
    // Arrange
    val validTracks = List(createValidTrack())
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test require(trackCount >= 0)
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = "session_001",
        userId = "user_000001",
        trackCount = -1,
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = validTracks
      )
    }
  }

  it should "reject null tracks list" in {
    // Arrange
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test require(tracks != null && tracks.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = "session_001",
        userId = "user_000001",
        trackCount = 1,
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = null
      )
    }
  }

  it should "reject empty tracks list" in {
    // Arrange
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test require(tracks.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = "session_001",
        userId = "user_000001",
        trackCount = 1,
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = List.empty
      )
    }
  }

  it should "reject inconsistent track count vs tracks list size" in {
    // Arrange
    val validTracks = List(createValidTrack("Track 1"), createValidTrack("Track 2"))
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test require(trackCount == tracks.size)
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = "session_001",
        userId = "user_000001",
        trackCount = 5, // Inconsistent with tracks.size = 2
        durationMinutes = 4L,
        startTime = startTime,
        endTime = endTime,
        tracks = validTracks
      )
    }
  }

  it should "reject negative duration" in {
    // Arrange
    val validTracks = List(createValidTrack())
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:11:57Z")

    // Act & Assert - Test require(durationMinutes >= 0)
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = "session_001",
        userId = "user_000001",
        trackCount = 1,
        durationMinutes = -5L, // Negative duration
        startTime = startTime,
        endTime = endTime,
        tracks = validTracks
      )
    }
  }

  /**
   * Tests for valid RankedSession creation scenarios.
   */
  "RankedSession valid creation" should "create session with single track" in {
    // Arrange
    val singleTrack = List(createValidTrack("Single Track"))
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = startTime // Same time for single track

    // Act
    val session = RankedSession(
      rank = 1,
      sessionId = "session_single",
      userId = "user_000001",
      trackCount = 1,
      durationMinutes = 0L,
      startTime = startTime,
      endTime = endTime,
      tracks = singleTrack
    )

    // Assert
    session.trackCount should be(1)
    session.tracks.size should be(1)
    session.durationMinutes should be(0L)
  }

  it should "create session with multiple tracks" in {
    // Arrange
    val multipleTracks = List(
      createValidTrack("Track 1"),
      createValidTrack("Track 2"),
      createValidTrack("Track 3")
    )
    val startTime = Instant.parse("2009-05-04T23:08:57Z")
    val endTime = Instant.parse("2009-05-04T23:20:57Z")

    // Act
    val session = RankedSession(
      rank = 5,
      sessionId = "session_multi",
      userId = "user_000001",
      trackCount = 3,
      durationMinutes = 12L,
      startTime = startTime,
      endTime = endTime,
      tracks = multipleTracks
    )

    // Assert
    session.trackCount should be(3)
    session.tracks.size should be(3)
    session.durationMinutes should be(12L)
  }

  it should "handle session with maximum track count" in {
    // Arrange - Simulate session with very high track count (power user scenario)
    val manyTracks = (1 to 100).map(i => createValidTrack(s"Track $i")).toList
    val startTime = Instant.parse("2009-05-04T00:00:00Z")
    val endTime = Instant.parse("2009-05-04T23:59:59Z")

    // Act
    val powerUserSession = RankedSession(
      rank = 1, // Top session due to high track count
      sessionId = "session_power_user",
      userId = "user_000001",
      trackCount = 100,
      durationMinutes = 23 * 60L + 59L, // 23 hours 59 minutes
      startTime = startTime,
      endTime = endTime,
      tracks = manyTracks
    )

    // Assert
    powerUserSession.trackCount should be(100)
    powerUserSession.tracks.size should be(100)
    powerUserSession.durationMinutes should be(23 * 60L + 59L)
  }
}
