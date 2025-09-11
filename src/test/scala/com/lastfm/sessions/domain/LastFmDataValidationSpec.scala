package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Test specification for Last.fm specific data validation functions.
 * 
 * Tests based on actual data patterns discovered in Last.fm dataset:
 * - All userIds follow "user_XXXXXX" format
 * - All timestamps are ISO 8601 format with perfect parsing
 * - 8 empty track names need default replacement
 * - Unicode artist names (坂本龍一, Sigur Rós) must be preserved
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class LastFmDataValidationSpec extends AnyFlatSpec with Matchers {

  /**
   * Tests for userId validation - based on actual Last.fm format.
   */
  "UserId validation" should "accept valid Last.fm user format" in {
    // Arrange & Act & Assert - Based on actual data format
    LastFmDataValidation.validateUserId("user_000001") should be(Valid("user_000001"))
    LastFmDataValidation.validateUserId("user_999999") should be(Valid("user_999999"))
    LastFmDataValidation.validateUserId("user_000274") should be(Valid("user_000274"))
  }
  
  it should "reject null userIds" in {
    // Act & Assert
    LastFmDataValidation.validateUserId(null) should be(Invalid("userId cannot be null"))
  }
  
  it should "reject empty userIds" in {
    // Act & Assert
    LastFmDataValidation.validateUserId("") should be(Invalid("userId cannot be empty"))
  }
  
  it should "reject blank userIds" in {
    // Act & Assert
    LastFmDataValidation.validateUserId("   ") should be(Invalid("userId cannot be blank"))
  }
  
  it should "reject invalid format userIds" in {
    // Act & Assert - Must follow Last.fm pattern
    LastFmDataValidation.validateUserId("invalid_format") should be(Invalid("userId must follow user_XXXXXX format"))
    LastFmDataValidation.validateUserId("user_abc") should be(Invalid("userId must follow user_XXXXXX format"))
    LastFmDataValidation.validateUserId("USER_000001") should be(Invalid("userId must follow user_XXXXXX format"))
  }
  
  it should "handle userIds with whitespace" in {
    // Act & Assert - Should trim and validate
    LastFmDataValidation.validateUserId("  user_000001  ") should be(Valid("user_000001"))
  }

  /**
   * Tests for timestamp validation - based on perfect ISO 8601 parsing in real data.
   */
  "Timestamp validation" should "parse valid ISO 8601 timestamps" in {
    // Arrange & Act & Assert - Based on actual data samples
    val result = LastFmDataValidation.validateTimestamp("2009-05-04T23:08:57Z")
    result should be(Valid(Instant.parse("2009-05-04T23:08:57Z")))
  }
  
  it should "handle Last.fm standard format timestamps" in {
    // Act & Assert - Focus on actual Last.fm format from notebook analysis
    val result = LastFmDataValidation.validateTimestamp("2009-05-04T23:08:57Z")
    result should be(Valid(Instant.parse("2009-05-04T23:08:57Z")))
    
    // Test another valid Last.fm timestamp
    val result2 = LastFmDataValidation.validateTimestamp("2009-05-04T13:54:10Z") 
    result2 should be(Valid(Instant.parse("2009-05-04T13:54:10Z")))
  }
  
  it should "reject null timestamps" in {
    // Act & Assert
    LastFmDataValidation.validateTimestamp(null) should be(Invalid("timestamp cannot be null"))
  }
  
  it should "reject empty timestamps" in {
    // Act & Assert
    LastFmDataValidation.validateTimestamp("") should be(Invalid("timestamp cannot be empty"))
  }
  
  it should "reject unparseable timestamps" in {
    // Act & Assert
    LastFmDataValidation.validateTimestamp("not-a-timestamp") should be(Invalid("Cannot parse timestamp"))
    LastFmDataValidation.validateTimestamp("2009-13-45T25:70:90Z") should be(Invalid("Cannot parse timestamp"))
  }
  
  it should "handle whitespace in timestamps" in {
    // Act & Assert
    val result = LastFmDataValidation.validateTimestamp("  2009-05-04T23:08:57Z  ")
    result should be(Valid(Instant.parse("2009-05-04T23:08:57Z")))
  }

  /**
   * Tests for track name validation - handling the 8 empty track names found.
   */
  "Track name validation" should "replace empty track names with default" in {
    // Act & Assert - Based on actual data issue
    LastFmDataValidation.validateTrackName("") should be(Valid("Unknown Track"))
    LastFmDataValidation.validateTrackName(null) should be(Valid("Unknown Track"))
    LastFmDataValidation.validateTrackName("   ") should be(Valid("Unknown Track")) // whitespace only
  }
  
  it should "preserve valid track names" in {
    // Arrange & Act & Assert - Based on actual data samples
    LastFmDataValidation.validateTrackName("Fuck Me Im Famous (Pacha Ibiza)-09-28-2007") should be(Valid("Fuck Me Im Famous (Pacha Ibiza)-09-28-2007"))
    LastFmDataValidation.validateTrackName("Composition 0919 (Live_2009_4_15)") should be(Valid("Composition 0919 (Live_2009_4_15)"))
    LastFmDataValidation.validateTrackName("The Last Emperor (Theme)") should be(Valid("The Last Emperor (Theme)"))
  }
  
  it should "trim whitespace from track names" in {
    // Act & Assert
    LastFmDataValidation.validateTrackName("  Some Track Name  ") should be(Valid("Some Track Name"))
    LastFmDataValidation.validateTrackName("\tTrack\n") should be(Valid("Track"))
  }
  
  it should "handle special characters in track names" in {
    // Act & Assert - Based on actual data patterns
    LastFmDataValidation.validateTrackName("Track (feat. Artist)") should be(Valid("Track (feat. Artist)"))
    LastFmDataValidation.validateTrackName("Track - Remix") should be(Valid("Track - Remix"))
    LastFmDataValidation.validateTrackName("Track & Another Track") should be(Valid("Track & Another Track"))
  }

  /**
   * Tests for artist name validation - preserving Unicode characters.
   */
  "Artist name validation" should "preserve Unicode artist names" in {
    // Arrange & Act & Assert - Based on actual data samples
    LastFmDataValidation.validateArtistName("坂本龍一") should be(Valid("坂本龍一"))
    LastFmDataValidation.validateArtistName("Sigur Rós") should be(Valid("Sigur Rós"))
    LastFmDataValidation.validateArtistName("Ennio Morricone") should be(Valid("Ennio Morricone"))
  }
  
  it should "handle null artist names gracefully" in {
    // Act & Assert - No null artist names in actual data, but handle defensively
    LastFmDataValidation.validateArtistName(null) should be(Valid("Unknown Artist"))
  }
  
  it should "handle empty artist names" in {
    // Act & Assert
    LastFmDataValidation.validateArtistName("") should be(Valid("Unknown Artist"))
    LastFmDataValidation.validateArtistName("   ") should be(Valid("Unknown Artist"))
  }
  
  it should "trim whitespace from artist names" in {
    // Act & Assert
    LastFmDataValidation.validateArtistName("  Deep Dish  ") should be(Valid("Deep Dish"))
    LastFmDataValidation.validateArtistName("\tUnderworld\n") should be(Valid("Underworld"))
  }
  
  it should "preserve valid artist names exactly" in {
    // Arrange & Act & Assert - Based on actual data samples
    LastFmDataValidation.validateArtistName("Deep Dish") should be(Valid("Deep Dish"))
    LastFmDataValidation.validateArtistName("Underworld") should be(Valid("Underworld"))
    LastFmDataValidation.validateArtistName("Beanfield") should be(Valid("Beanfield"))
  }

  /**
   * Tests for track key generation - MBID preferred with fallback strategy.
   */
  "Track key generation" should "use track MBID when available" in {
    // Arrange & Act - Based on actual data with MBIDs
    val trackKey = LastFmDataValidation.generateTrackKey(
      trackId = Some("f7c1f8f8-b935-45ed-8fc8-7def69d92a10"),
      artistName = "坂本龍一",
      trackName = "The Last Emperor (Theme)"
    )
    
    // Assert - Should use MBID directly
    trackKey should be("f7c1f8f8-b935-45ed-8fc8-7def69d92a10")
  }
  
  it should "fall back to artist-track combination when no MBID" in {
    // Arrange & Act - Based on data without track MBIDs
    val trackKey = LastFmDataValidation.generateTrackKey(
      trackId = None,
      artistName = "Deep Dish",
      trackName = "Fuck Me Im Famous (Pacha Ibiza)-09-28-2007"
    )
    
    // Assert - Should create composite key
    trackKey should be("Deep Dish — Fuck Me Im Famous (Pacha Ibiza)-09-28-2007")
  }
  
  it should "handle empty MBID strings as missing" in {
    // Arrange & Act
    val trackKey = LastFmDataValidation.generateTrackKey(
      trackId = Some(""),  // Empty string should be treated as None
      artistName = "Artist",
      trackName = "Track"
    )
    
    // Assert - Should fall back to name combination
    trackKey should be("Artist — Track")
  }
  
  it should "handle whitespace-only MBID strings as missing" in {
    // Arrange & Act
    val trackKey = LastFmDataValidation.generateTrackKey(
      trackId = Some("   "),
      artistName = "Artist",
      trackName = "Track"
    )
    
    // Assert - Should fall back to name combination
    trackKey should be("Artist — Track")
  }
  
  it should "use cleaned artist and track names in composite key" in {
    // Arrange & Act
    val trackKey = LastFmDataValidation.generateTrackKey(
      trackId = None,
      artistName = "  Artist Name  ",
      trackName = "\tTrack Name\n"
    )
    
    // Assert - Should use cleaned names
    trackKey should be("Artist Name — Track Name")
  }
}