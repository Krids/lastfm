package com.lastfm.sessions.domain.validation

import com.lastfm.sessions.domain.ListenEvent
import com.lastfm.sessions.utils.BaseTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Comprehensive validation test specification for ListenEvent domain model.
 * 
 * Tests all constructor validation rules to achieve complete branch coverage
 * of the ListenEvent class validation logic (13 require statements).
 * 
 * Coverage Target: All 13 require statements + edge case handling
 * Expected Impact: +5% statement coverage, +4% branch coverage
 * 
 * Follows TDD principles with exhaustive validation testing for critical
 * listening event data integrity.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class ListenEventValidationSpec extends AnyFlatSpec with BaseTestSpec with Matchers {

  /**
   * Tests for userId validation rules (3 require statements).
   */
  "ListenEvent userId validation" should "reject null userId" in {
    // Act & Assert - Test require(userId != null)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = null,
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "Valid Track",
        trackKey = "Valid — Track"
      )
    }
  }

  it should "reject empty userId" in {
    // Act & Assert - Test require(userId.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "Valid Track",
        trackKey = "Valid — Track"
      )
    }
  }

  it should "reject blank userId" in {
    // Act & Assert - Test require(!userId.isBlank)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "   ",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "Valid Track",
        trackKey = "Valid — Track"
      )
    }
  }

  /**
   * Tests for timestamp validation rule (1 require statement).
   */
  "ListenEvent timestamp validation" should "reject null timestamp" in {
    // Act & Assert - Test require(timestamp != null)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = null,
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "Valid Track",
        trackKey = "Valid — Track"
      )
    }
  }

  /**
   * Tests for artistName validation rules (3 require statements).
   */
  "ListenEvent artistName validation" should "reject null artistName" in {
    // Act & Assert - Test require(artistName != null)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = null,
        trackId = None,
        trackName = "Valid Track",
        trackKey = "Valid — Track"
      )
    }
  }

  it should "reject empty artistName" in {
    // Act & Assert - Test require(artistName.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "",
        trackId = None,
        trackName = "Valid Track",
        trackKey = "Valid — Track"
      )
    }
  }

  it should "reject blank artistName" in {
    // Act & Assert - Test require(!artistName.isBlank)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "   ",
        trackId = None,
        trackName = "Valid Track",
        trackKey = "Valid — Track"
      )
    }
  }

  /**
   * Tests for trackName validation rules (3 require statements).
   */
  "ListenEvent trackName validation" should "reject null trackName" in {
    // Act & Assert - Test require(trackName != null)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = null,
        trackKey = "Valid — Track"
      )
    }
  }

  it should "reject empty trackName" in {
    // Act & Assert - Test require(trackName.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "",
        trackKey = "Valid — Track"
      )
    }
  }

  it should "reject blank trackName" in {
    // Act & Assert - Test require(!trackName.isBlank)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "   ",
        trackKey = "Valid — Track"
      )
    }
  }

  /**
   * Tests for trackKey validation rules (3 require statements).
   */
  "ListenEvent trackKey validation" should "reject null trackKey" in {
    // Act & Assert - Test require(trackKey != null)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "Valid Track",
        trackKey = null
      )
    }
  }

  it should "reject empty trackKey" in {
    // Act & Assert - Test require(trackKey.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "Valid Track",
        trackKey = ""
      )
    }
  }

  it should "reject blank trackKey" in {
    // Act & Assert - Test require(!trackKey.isBlank)
    an[IllegalArgumentException] should be thrownBy {
      ListenEvent(
        userId = "user_000001",
        timestamp = Instant.now(),
        artistId = None,
        artistName = "Valid Artist",
        trackId = None,
        trackName = "Valid Track",
        trackKey = "   "
      )
    }
  }

  /**
   * Tests for valid ListenEvent creation scenarios.
   * These tests ensure the happy path works correctly after validation.
   */
  "ListenEvent valid creation" should "create minimal valid ListenEvent" in {
    // Arrange - Minimal valid parameters
    val timestamp = Instant.parse("2009-05-04T23:08:57Z")
    
    // Act
    val event = ListenEvent(
      userId = "user_000001",
      timestamp = timestamp,
      artistId = None,
      artistName = "Test Artist",
      trackId = None,
      trackName = "Test Track",
      trackKey = "Test Artist — Test Track"
    )

    // Assert - Should create successfully with all fields
    event.userId should be("user_000001")
    event.timestamp should be(timestamp)
    event.artistId should be(None)
    event.artistName should be("Test Artist")
    event.trackId should be(None)
    event.trackName should be("Test Track")
    event.trackKey should be("Test Artist — Test Track")
  }

  it should "create complete ListenEvent with all optional fields" in {
    // Arrange - Complete parameters including optional IDs
    val timestamp = Instant.parse("2009-05-04T23:08:57Z")
    
    // Act
    val event = ListenEvent(
      userId = "user_000001",
      timestamp = timestamp,
      artistId = Some("artist-mbid-123"),
      artistName = "Complete Artist",
      trackId = Some("track-mbid-456"),
      trackName = "Complete Track",
      trackKey = "track-mbid-456" // Using track MBID as key
    )

    // Assert - Should preserve all fields including optional ones
    event.userId should be("user_000001")
    event.timestamp should be(timestamp)
    event.artistId should be(Some("artist-mbid-123"))
    event.artistName should be("Complete Artist")
    event.trackId should be(Some("track-mbid-456"))
    event.trackName should be("Complete Track")
    event.trackKey should be("track-mbid-456")
  }

  /**
   * Tests for realistic Last.fm data scenarios and edge cases.
   */
  "ListenEvent Last.fm data scenarios" should "handle Last.fm user ID format" in {
    // Arrange - Typical Last.fm user ID format
    val event = ListenEvent(
      userId = "user_000123",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = Some("83d91898-7763-47d7-b03b-b92132375c47"),
      artistName = "Radiohead",
      trackId = Some("4f9b8675-3d73-4cc9-b4ed-bd1a56c29dd8"),
      trackName = "Paranoid Android",
      trackKey = "4f9b8675-3d73-4cc9-b4ed-bd1a56c29dd8"
    )

    // Act & Assert - Should handle typical Last.fm patterns
    event.userId should startWith("user_")
    event.artistId.get should have length 36 // UUID format
    event.trackId.get should have length 36  // UUID format
  }

  it should "handle international artist and track names" in {
    // Arrange - International content with Unicode characters
    val internationalEvent = ListenEvent(
      userId = "user_000456",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = None,
      artistName = "坂本龍一", // Japanese artist name
      trackId = None,
      trackName = "Merry Christmas Mr. Lawrence", // English track name
      trackKey = "坂本龍一 — Merry Christmas Mr. Lawrence"
    )

    val nordicEvent = ListenEvent(
      userId = "user_000789",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = None,
      artistName = "Sigur Rós", // Icelandic artist with special characters
      trackId = None,
      trackName = "Hoppípolla",
      trackKey = "Sigur Rós — Hoppípolla"
    )

    // Act & Assert - Should preserve Unicode characters correctly
    internationalEvent.artistName should be("坂本龍一")
    internationalEvent.trackKey should include("坂本龍一")

    nordicEvent.artistName should be("Sigur Rós")
    nordicEvent.trackKey should include("Sigur Rós")
  }

  it should "handle complex track metadata with special characters" in {
    // Arrange - Complex track with various special characters
    val complexEvent = ListenEvent(
      userId = "user_001000",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = None,
      artistName = "Artist & The Band (feat. Guest)",
      trackId = None,
      trackName = "Song Title (Remix) [Radio Edit] - Extended Version",
      trackKey = "Artist & The Band (feat. Guest) — Song Title (Remix) [Radio Edit] - Extended Version"
    )

    // Act & Assert - Should handle complex metadata correctly
    complexEvent.artistName should include("&")
    complexEvent.artistName should include("(feat.")
    complexEvent.trackName should include("(Remix)")
    complexEvent.trackName should include("[Radio Edit]")
    complexEvent.trackName should include("Extended Version")
    complexEvent.trackKey should include("—") // Track key separator
  }

  it should "handle edge case timing scenarios" in {
    // Arrange - Various timestamp edge cases
    val epochEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.EPOCH, // January 1, 1970 00:00:00 UTC
      artistId = None,
      artistName = "Historical Artist",
      trackId = None,
      trackName = "Ancient Track",
      trackKey = "Historical Artist — Ancient Track"
    )

    val recentEvent = ListenEvent(
      userId = "user_000002",
      timestamp = Instant.parse("2024-12-31T23:59:59Z"), // Recent timestamp
      artistId = None,
      artistName = "Modern Artist",
      trackId = None,
      trackName = "Latest Hit",
      trackKey = "Modern Artist — Latest Hit"
    )

    val preciseEvent = ListenEvent(
      userId = "user_000003",
      timestamp = Instant.parse("2009-05-04T23:08:57.123456789Z"), // Nanosecond precision
      artistId = None,
      artistName = "Precise Artist",
      trackId = None,
      trackName = "Precise Track",
      trackKey = "Precise Artist — Precise Track"
    )

    // Act & Assert - Should handle various timestamp formats and ranges
    epochEvent.timestamp should be(Instant.EPOCH)
    recentEvent.timestamp.getEpochSecond should be > 1000000000L // Recent timestamp
    preciseEvent.timestamp.getNano should be(123456789) // Nanosecond precision preserved
  }

  /**
   * Tests for optional field handling edge cases.
   */
  "ListenEvent optional fields" should "handle None values for optional IDs" in {
    // Arrange - Event with no MBID data (common scenario)
    val noMbidEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = None,
      artistName = "No MBID Artist",
      trackId = None,
      trackName = "No MBID Track", 
      trackKey = "No MBID Artist — No MBID Track" // Fallback key format
    )

    // Act & Assert - Should handle missing MBID data gracefully
    noMbidEvent.artistId should be(None)
    noMbidEvent.trackId should be(None)
    noMbidEvent.trackKey should include("—") // Fallback separator
  }

  it should "handle Some values for optional IDs" in {
    // Arrange - Event with complete MBID data (preferred scenario)
    val completeMbidEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = Some("artist-mbid-789"),
      artistName = "MBID Artist",
      trackId = Some("track-mbid-012"),
      trackName = "MBID Track",
      trackKey = "track-mbid-012" // Using track MBID as key
    )

    // Act & Assert - Should preserve MBID data when available
    completeMbidEvent.artistId should be(Some("artist-mbid-789"))
    completeMbidEvent.trackId should be(Some("track-mbid-012"))
    completeMbidEvent.trackKey should be("track-mbid-012") // MBID takes precedence
  }

  it should "handle partial MBID data scenarios" in {
    // Arrange - Event with artist MBID but no track MBID
    val partialMbidEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = Some("artist-mbid-345"),
      artistName = "Partial MBID Artist",
      trackId = None,
      trackName = "No Track MBID",
      trackKey = "artist-mbid-345 — No Track MBID" // Hybrid key format
    )

    // Act & Assert - Should handle partial MBID scenarios
    partialMbidEvent.artistId should be(Some("artist-mbid-345"))
    partialMbidEvent.trackId should be(None)
    partialMbidEvent.trackKey should include("artist-mbid-345")
    partialMbidEvent.trackKey should include("No Track MBID")
  }

  /**
   * Tests for realistic data quality scenarios from Last.fm dataset.
   */
  "ListenEvent data quality scenarios" should "handle empty track name replacement scenario" in {
    // Note: This test validates that the ListenEvent can be created with cleaned data
    // The actual empty track name cleaning happens in data validation layer
    
    // Arrange - Event after data cleaning has replaced empty track name
    val cleanedEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = None,
      artistName = "Artist With Missing Track",
      trackId = None,
      trackName = "Unknown Track", // Cleaned from empty string
      trackKey = "Artist With Missing Track — Unknown Track"
    )

    // Act & Assert - Should accept cleaned data
    cleanedEvent.trackName should be("Unknown Track")
    cleanedEvent.trackKey should include("Unknown Track")
  }

  it should "handle duplicate detection scenario" in {
    // Arrange - Two identical events (would be duplicates in raw data)
    val timestamp = Instant.parse("2009-05-04T23:08:57Z")
    
    val event1 = ListenEvent(
      userId = "user_000001",
      timestamp = timestamp,
      artistId = Some("same-artist-id"),
      artistName = "Same Artist",
      trackId = Some("same-track-id"),
      trackName = "Same Track",
      trackKey = "same-track-id"
    )

    val event2 = ListenEvent(
      userId = "user_000001",
      timestamp = timestamp,
      artistId = Some("same-artist-id"),
      artistName = "Same Artist",
      trackId = Some("same-track-id"),
      trackName = "Same Track",
      trackKey = "same-track-id"
    )

    // Act & Assert - Should create identical events (deduplication handled elsewhere)
    event1.userId should be(event2.userId)
    event1.timestamp should be(event2.timestamp)
    event1.trackKey should be(event2.trackKey)
    event1 should be(event2) // Case class equality
  }

  it should "handle high-frequency user listening patterns" in {
    // Arrange - Events representing rapid listening (potential bot/power user)
    val timestamp1 = Instant.parse("2009-05-04T23:08:57Z")
    val timestamp2 = Instant.parse("2009-05-04T23:09:00Z") // 3 seconds later
    
    val rapidEvent1 = ListenEvent(
      userId = "user_999999", // Potential power user
      timestamp = timestamp1,
      artistId = None,
      artistName = "High Frequency Artist",
      trackId = None,
      trackName = "Track 1",
      trackKey = "High Frequency Artist — Track 1"
    )

    val rapidEvent2 = ListenEvent(
      userId = "user_999999",
      timestamp = timestamp2,
      artistId = None,
      artistName = "High Frequency Artist",
      trackId = None,
      trackName = "Track 2",
      trackKey = "High Frequency Artist — Track 2"
    )

    // Act & Assert - Should handle rapid listening patterns
    val timeDiff = java.time.Duration.between(rapidEvent1.timestamp, rapidEvent2.timestamp)
    timeDiff.getSeconds should be(3L)
    
    rapidEvent1.userId should be(rapidEvent2.userId)
    rapidEvent1.trackKey should not be rapidEvent2.trackKey
  }

  /**
   * Tests for edge cases in track key generation and identity resolution.
   */
  "ListenEvent track identity scenarios" should "handle track key with MBID preference" in {
    // Arrange - Event where track MBID should be preferred for key
    val mbidPreferenceEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = Some("artist-id-123"),
      artistName = "Artist Name",
      trackId = Some("track-id-456"),
      trackName = "Track Name",
      trackKey = "track-id-456" // Track MBID takes precedence
    )

    // Act & Assert - Track MBID should be used as key
    mbidPreferenceEvent.trackKey should be("track-id-456")
    mbidPreferenceEvent.trackKey should not include("—")
    mbidPreferenceEvent.trackKey should not include("Artist Name")
    mbidPreferenceEvent.trackKey should not include("Track Name")
  }

  it should "handle track key with fallback format" in {
    // Arrange - Event with no track MBID, using fallback format
    val fallbackKeyEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = Some("artist-id-123"),
      artistName = "Fallback Artist",
      trackId = None, // No track MBID
      trackName = "Fallback Track",
      trackKey = "Fallback Artist — Fallback Track" // Fallback format
    )

    // Act & Assert - Should use artist — track format when no track MBID
    fallbackKeyEvent.trackKey should include("—")
    fallbackKeyEvent.trackKey should include("Fallback Artist")
    fallbackKeyEvent.trackKey should include("Fallback Track")
  }

  it should "handle extremely long metadata within validation constraints" in {
    // Arrange - Event with very long but valid metadata
    val longMetadata = "Very Long Artist Name That Exceeds Normal Length But Is Still Valid"
    val longTrackName = "Very Long Track Name That Also Exceeds Normal Length But Remains Valid"
    
    val longMetadataEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = None,
      artistName = longMetadata,
      trackId = None,
      trackName = longTrackName,
      trackKey = s"$longMetadata — $longTrackName"
    )

    // Act & Assert - Should handle long metadata correctly
    longMetadataEvent.artistName.length should be > 50
    longMetadataEvent.trackName.length should be > 50
    longMetadataEvent.trackKey.length should be > 100
    longMetadataEvent.trackKey should include("—")
  }
}
