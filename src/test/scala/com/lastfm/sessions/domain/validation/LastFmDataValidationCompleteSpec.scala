package com.lastfm.sessions.domain.validation

import com.lastfm.sessions.domain.{LastFmDataValidation, Valid, Invalid}
import com.lastfm.sessions.utils.BaseTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Comprehensive validation test specification for LastFmDataValidation object.
 * 
 * Tests all validation methods to achieve complete branch coverage of the
 * Last.fm specific data validation logic including edge cases and error paths.
 * 
 * Coverage Target: All validation methods with complete branch coverage
 * Expected Impact: +4% statement coverage, +5% branch coverage
 * 
 * Focuses on Last.fm specific data patterns and validation rules established
 * through dataset analysis (user_XXXXXX format, ISO 8601 timestamps, etc.).
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class LastFmDataValidationCompleteSpec extends AnyFlatSpec with BaseTestSpec with Matchers {

  /**
   * Tests for validateUserId method with complete branch coverage.
   * Covers all conditional branches in user ID validation logic.
   */
  "LastFmDataValidation.validateUserId" should "accept valid Last.fm user ID formats" in {
    // Arrange & Act - Test valid user ID patterns
    val result1 = LastFmDataValidation.validateUserId("user_123456")
    val result2 = LastFmDataValidation.validateUserId("user_000001")
    val result3 = LastFmDataValidation.validateUserId("user_999999")

    // Assert - Should accept valid formats
    result1 should be(Valid("user_123456"))
    result2 should be(Valid("user_000001"))
    result3 should be(Valid("user_999999"))
  }

  it should "reject null userId" in {
    // Act & Assert - Test if (userId == null) branch
    val result = LastFmDataValidation.validateUserId(null)
    result should be(Invalid("userId cannot be null"))
  }

  it should "reject empty userId" in {
    // Act & Assert - Test else if (userId.isEmpty) branch
    val result = LastFmDataValidation.validateUserId("")
    result should be(Invalid("userId cannot be empty"))
  }

  it should "reject blank userId" in {
    // Act & Assert - Test else if (userId.isBlank) branch
    val result = LastFmDataValidation.validateUserId("   ")
    result should be(Invalid("userId cannot be blank"))
  }

  it should "reject invalid user ID format patterns" in {
    // Act & Assert - Test !UserIdPattern.matches(trimmedUserId) branch
    
    // Missing underscore
    LastFmDataValidation.validateUserId("user123456") should be(Invalid("userId must follow user_XXXXXX format"))
    
    // Wrong prefix
    LastFmDataValidation.validateUserId("person_123456") should be(Invalid("userId must follow user_XXXXXX format"))
    
    // Too few digits
    LastFmDataValidation.validateUserId("user_12345") should be(Invalid("userId must follow user_XXXXXX format"))
    
    // Too many digits
    LastFmDataValidation.validateUserId("user_1234567") should be(Invalid("userId must follow user_XXXXXX format"))
    
    // Non-digit characters
    LastFmDataValidation.validateUserId("user_12345a") should be(Invalid("userId must follow user_XXXXXX format"))
    LastFmDataValidation.validateUserId("user_abcdef") should be(Invalid("userId must follow user_XXXXXX format"))
    
    // Special characters in digits
    LastFmDataValidation.validateUserId("user_123-45") should be(Invalid("userId must follow user_XXXXXX format"))
    LastFmDataValidation.validateUserId("user_123 45") should be(Invalid("userId must follow user_XXXXXX format"))
  }

  it should "handle whitespace trimming in valid userIds" in {
    // Act & Assert - Test trimmedUserId = userId.trim processing
    val result = LastFmDataValidation.validateUserId("  user_123456  ")
    result should be(Valid("user_123456"))
    
    val resultWithTabs = LastFmDataValidation.validateUserId("\tuser_654321\n")
    resultWithTabs should be(Valid("user_654321"))
  }

  /**
   * Tests for validateTimestamp method with complete branch coverage.
   * Covers all conditional branches and ISO 8601 parsing scenarios.
   */
  "LastFmDataValidation.validateTimestamp" should "accept valid ISO 8601 timestamp formats" in {
    // Arrange & Act - Test various valid ISO 8601 formats
    val basicResult = LastFmDataValidation.validateTimestamp("2009-05-04T23:08:57Z")
    val millisResult = LastFmDataValidation.validateTimestamp("2009-05-04T23:08:57.123Z")
    // Assert - Should parse basic ISO 8601 formats successfully
    basicResult.isInstanceOf[Valid[_]] should be(true)
    millisResult.isInstanceOf[Valid[_]] should be(true)

    // Verify actual Instant values
    basicResult.asInstanceOf[Valid[Instant]].value should be(Instant.parse("2009-05-04T23:08:57Z"))
  }

  it should "reject timezone offset formats not supported by Instant.parse" in {
    // Act & Assert - Instant.parse only supports Z format, not offset formats
    val offsetResult = LastFmDataValidation.validateTimestamp("2009-05-04T23:08:57+02:00")
    val negativeOffsetResult = LastFmDataValidation.validateTimestamp("2009-05-04T23:08:57-05:00")

    offsetResult should be(Invalid("Cannot parse timestamp"))
    negativeOffsetResult should be(Invalid("Cannot parse timestamp"))
  }

  it should "reject null timestamp" in {
    // Act & Assert - Test if (timestampStr == null) branch
    val result = LastFmDataValidation.validateTimestamp(null)
    result should be(Invalid("timestamp cannot be null"))
  }

  it should "reject empty timestamp after trimming" in {
    // Act & Assert - Test if (trimmedTimestamp.isEmpty) branch
    val emptyResult = LastFmDataValidation.validateTimestamp("")
    val blankResult = LastFmDataValidation.validateTimestamp("   ")

    emptyResult should be(Invalid("timestamp cannot be empty"))
    blankResult should be(Invalid("timestamp cannot be empty"))
  }

  it should "reject unparseable timestamp formats" in {
    // Act & Assert - Test Failure(_) => Invalid("Cannot parse timestamp") branch
    val invalidFormats = List(
      "invalid-timestamp",
      "2009-05-04", // Missing time component
      "23:08:57Z",  // Missing date component
      "2009/05/04 23:08:57", // Wrong separators
      "2009-13-04T23:08:57Z", // Invalid month
      "2009-05-32T23:08:57Z", // Invalid day
      "2009-05-04T25:08:57Z", // Invalid hour
      "2009-05-04T23:61:57Z", // Invalid minute
      "2009-05-04T23:08:61Z", // Invalid second
      "not-a-timestamp-at-all"
    )

    invalidFormats.foreach { invalidTimestamp =>
      val result = LastFmDataValidation.validateTimestamp(invalidTimestamp)
      result should be(Invalid("Cannot parse timestamp"))
    }
  }

  it should "handle whitespace trimming in timestamps" in {
    // Act & Assert - Test trimmedTimestamp = timestampStr.trim processing
    val result = LastFmDataValidation.validateTimestamp("  2009-05-04T23:08:57Z  ")
    result should be(Valid(Instant.parse("2009-05-04T23:08:57Z")))
    
    val resultWithNewlines = LastFmDataValidation.validateTimestamp("\n2009-05-04T23:08:57Z\t")
    resultWithNewlines should be(Valid(Instant.parse("2009-05-04T23:08:57Z")))
  }

  it should "handle leap year and date edge cases" in {
    // Arrange & Act - Test edge cases in date parsing
    val leapYearResult = LastFmDataValidation.validateTimestamp("2008-02-29T00:00:00Z") // Valid leap year
    val nonLeapYearResult = LastFmDataValidation.validateTimestamp("2009-02-29T00:00:00Z") // Invalid leap year

    // Assert
    leapYearResult.isInstanceOf[Valid[_]] should be(true)
    nonLeapYearResult should be(Invalid("Cannot parse timestamp"))
  }

  /**
   * Tests for validateTrackName method with complete branch coverage.
   * This method uses defensive programming and never fails (always returns Valid).
   */
  "LastFmDataValidation.validateTrackName" should "handle null track name with default replacement" in {
    // Act & Assert - Test Option(trackName) with null
    val result = LastFmDataValidation.validateTrackName(null)
    result should be(Valid("Unknown Track"))
  }

  it should "handle empty track name with default replacement" in {
    // Act & Assert - Test .filter(_.trim.nonEmpty) with empty string
    val result = LastFmDataValidation.validateTrackName("")
    result should be(Valid("Unknown Track"))
  }

  it should "handle blank track name with default replacement" in {
    // Act & Assert - Test .filter(_.trim.nonEmpty) with whitespace
    val result = LastFmDataValidation.validateTrackName("   ")
    result should be(Valid("Unknown Track"))
  }

  it should "clean and return valid track names" in {
    // Act & Assert - Test .map(_.trim) processing for valid names
    val normalResult = LastFmDataValidation.validateTrackName("Good Track Name")
    val trimmedResult = LastFmDataValidation.validateTrackName("  Track With Spaces  ")

    normalResult should be(Valid("Good Track Name"))
    trimmedResult should be(Valid("Track With Spaces"))
  }

  it should "handle special characters and Unicode in track names" in {
    // Arrange & Act - Test various character sets
    val unicodeResult = LastFmDataValidation.validateTrackName("トラック名")
    val specialCharResult = LastFmDataValidation.validateTrackName("Track/Name (Remix) - Extended")
    val emojiResult = LastFmDataValidation.validateTrackName("Track 🎵 With Emoji")

    // Assert - Should preserve all character types
    unicodeResult should be(Valid("トラック名"))
    specialCharResult should be(Valid("Track/Name (Remix) - Extended"))
    emojiResult should be(Valid("Track 🎵 With Emoji"))
  }

  /**
   * Tests for validateArtistName method with complete branch coverage.
   * Similar to validateTrackName, this method is defensive and never fails.
   */
  "LastFmDataValidation.validateArtistName" should "handle null artist name with default replacement" in {
    // Act & Assert - Test Option(artistName) with null
    val result = LastFmDataValidation.validateArtistName(null)
    result should be(Valid("Unknown Artist"))
  }

  it should "handle empty artist name with default replacement" in {
    // Act & Assert - Test .filter(_.trim.nonEmpty) with empty string
    val result = LastFmDataValidation.validateArtistName("")
    result should be(Valid("Unknown Artist"))
  }

  it should "handle blank artist name with default replacement" in {
    // Act & Assert - Test .filter(_.trim.nonEmpty) with whitespace
    val result = LastFmDataValidation.validateArtistName("   ")
    result should be(Valid("Unknown Artist"))
  }

  it should "clean and return valid artist names" in {
    // Act & Assert - Test .map(_.trim) processing for valid names
    val normalResult = LastFmDataValidation.validateArtistName("Good Artist Name")
    val trimmedResult = LastFmDataValidation.validateArtistName("  Artist With Spaces  ")

    normalResult should be(Valid("Good Artist Name"))
    trimmedResult should be(Valid("Artist With Spaces"))
  }

  it should "handle international artists with Unicode characters" in {
    // Arrange & Act - Test real international artist names from Last.fm dataset
    val japaneseResult = LastFmDataValidation.validateArtistName("坂本龍一") // Ryuichi Sakamoto
    val icelandicResult = LastFmDataValidation.validateArtistName("Sigur Rós")
    val greekResult = LastFmDataValidation.validateArtistName("Μ-Ziq")
    val cyrillicResult = LastFmDataValidation.validateArtistName("Проверочная Линейка")

    // Assert - Should preserve international characters correctly
    japaneseResult should be(Valid("坂本龍一"))
    icelandicResult should be(Valid("Sigur Rós"))
    greekResult should be(Valid("Μ-Ziq"))
    cyrillicResult should be(Valid("Проверочная Линейка"))
  }

  it should "handle complex artist naming conventions" in {
    // Arrange & Act - Test various artist naming patterns
    val bandResult = LastFmDataValidation.validateArtistName("The Beatles")
    val collaborationResult = LastFmDataValidation.validateArtistName("Artist & The Band")
    val featuringResult = LastFmDataValidation.validateArtistName("Main Artist feat. Guest Artist")
    val compilationResult = LastFmDataValidation.validateArtistName("Various Artists")

    // Assert - Should handle all naming conventions
    bandResult should be(Valid("The Beatles"))
    collaborationResult should be(Valid("Artist & The Band"))
    featuringResult should be(Valid("Main Artist feat. Guest Artist"))
    compilationResult should be(Valid("Various Artists"))
  }

  /**
   * Tests for edge cases across all validation methods.
   */
  "LastFmDataValidation comprehensive edge cases" should "handle extreme whitespace scenarios" in {
    // Arrange & Act - Test various whitespace patterns
    val tabUserId = LastFmDataValidation.validateUserId("\tuser_123456\t")
    val newlineTimestamp = LastFmDataValidation.validateTimestamp("\n2009-05-04T23:08:57Z\r")
    val multiSpaceTrack = LastFmDataValidation.validateTrackName("  Track   With   Spaces  ")
    val mixedSpaceArtist = LastFmDataValidation.validateArtistName("\t  Artist\n  Name  \r")

    // Assert - Should handle and clean various whitespace patterns
    tabUserId should be(Valid("user_123456"))
    newlineTimestamp should be(Valid(Instant.parse("2009-05-04T23:08:57Z")))
    multiSpaceTrack should be(Valid("Track   With   Spaces")) // Internal spaces preserved
    mixedSpaceArtist should be(Valid("Artist\n  Name")) // Some internal whitespace preserved
  }

  it should "handle boundary timestamp formats" in {
    // Arrange & Act - Test timestamp parsing edge cases
    val epochResult = LastFmDataValidation.validateTimestamp("1970-01-01T00:00:00Z")
    val y2kResult = LastFmDataValidation.validateTimestamp("2000-01-01T00:00:00Z")
    val leap2000Result = LastFmDataValidation.validateTimestamp("2000-02-29T00:00:00Z")
    val futureResult = LastFmDataValidation.validateTimestamp("2030-12-31T23:59:59Z")

    // Assert - Should handle historical and future timestamps with Z format
    epochResult.isInstanceOf[Valid[_]] should be(true)
    y2kResult.isInstanceOf[Valid[_]] should be(true)
    leap2000Result.isInstanceOf[Valid[_]] should be(true)
    futureResult.isInstanceOf[Valid[_]] should be(true)
  }

  it should "handle user ID format edge cases" in {
    // Arrange & Act - Test edge cases around user ID pattern matching
    
    // Case sensitivity (should fail - lowercase 'user')
    val lowercaseResult = LastFmDataValidation.validateUserId("USER_123456")
    val mixedCaseResult = LastFmDataValidation.validateUserId("User_123456")
    
    // Leading zeros (should succeed)
    val leadingZerosResult = LastFmDataValidation.validateUserId("user_000001")
    
    // All zeros (should succeed)
    val allZerosResult = LastFmDataValidation.validateUserId("user_000000")

    // Assert
    lowercaseResult should be(Invalid("userId must follow user_XXXXXX format"))
    mixedCaseResult should be(Invalid("userId must follow user_XXXXXX format"))
    leadingZerosResult should be(Valid("user_000001"))
    allZerosResult should be(Valid("user_000000"))
  }

  /**
   * Tests for realistic Last.fm data scenarios based on dataset analysis.
   */
  "LastFmDataValidation Last.fm dataset scenarios" should "handle actual user ID samples from dataset" in {
    // Arrange - Real user ID patterns from Last.fm dataset
    val userIds = List(
      "user_000001",
      "user_000002", 
      "user_000003",
      "user_001000",
      "user_999999"
    )

    // Act & Assert - All should be valid Last.fm format
    userIds.foreach { userId =>
      val result = LastFmDataValidation.validateUserId(userId)
      result shouldBe a[Valid[_]]
      result.asInstanceOf[Valid[String]].value should be(userId)
    }
  }

  it should "handle actual timestamp samples from dataset" in {
    // Arrange - Real timestamp patterns from Last.fm dataset
    val timestamps = List(
      "2009-05-04T23:08:57Z",
      "2009-05-04T23:09:06Z",
      "2009-05-04T23:09:14Z",
      "2009-05-04T23:10:13Z",
      "2009-05-04T23:10:33Z"
    )

    // Act & Assert - All should parse correctly
    timestamps.foreach { timestamp =>
      val result = LastFmDataValidation.validateTimestamp(timestamp)
      result shouldBe a[Valid[_]]
      val parsedInstant = result.asInstanceOf[Valid[Instant]].value
      parsedInstant.toString should be(timestamp)
    }
  }

  it should "handle actual artist name samples with Unicode" in {
    // Arrange - Real artist names from Last.fm dataset analysis
    val artistNames = List(
      "Deep Dish",
      "The Knife", 
      "King Crimson",
      "坂本龍一", // Japanese
      "Sigur Rós", // Icelandic
      "Μ-Ziq",     // Greek
      "Проверочная Линейка" // Cyrillic
    )

    // Act & Assert - All should be preserved correctly
    artistNames.foreach { artistName =>
      val result = LastFmDataValidation.validateArtistName(artistName)
      result should be(Valid(artistName))
    }
  }

  it should "handle track name samples including edge cases" in {
    // Arrange - Real track names and edge cases from dataset
    val trackNames = List(
      "Heartbeats",
      "Epitaph",
      "Flashdance", 
      "Track Name (Remix)",
      "Song - Live Version",
      "Title feat. Guest Artist",
      "", // Empty track name (8 cases in 19M records)
      "   " // Blank track name
    )

    // Act & Assert - Should handle all cases with appropriate defaults
    LastFmDataValidation.validateTrackName("Heartbeats") should be(Valid("Heartbeats"))
    LastFmDataValidation.validateTrackName("Track Name (Remix)") should be(Valid("Track Name (Remix)"))
    LastFmDataValidation.validateTrackName("") should be(Valid("Unknown Track"))
    LastFmDataValidation.validateTrackName("   ") should be(Valid("Unknown Track"))
  }

  /**
   * Tests for defensive programming edge cases.
   */
  "LastFmDataValidation defensive programming" should "never throw exceptions from track validation" in {
    // Arrange - Extreme edge cases that should never cause exceptions
    val extremeCases = List(
      null,
      "",
      "   ",
      "a" * 10000, // Very long string
      "\u0000\u0001\u0002", // Control characters
      "🎵🎤🎸🥁", // Emoji only
      "Normal Track Name"
    )

    // Act & Assert - Should never throw, always return Valid result
    extremeCases.foreach { trackName =>
      val result = LastFmDataValidation.validateTrackName(trackName)
      result shouldBe a[Valid[_]]
      val cleanedName = result.asInstanceOf[Valid[String]].value
      cleanedName should not be null
      cleanedName.trim should not be empty
    }
  }

  it should "never throw exceptions from artist validation" in {
    // Arrange - Extreme edge cases for artist names
    val extremeCases = List(
      null,
      "",
      "   ",
      "a" * 10000, // Very long string
      "\u0000\u0001\u0002", // Control characters
      "🎵🎤🎸🥁", // Emoji only
      "Normal Artist Name"
    )

    // Act & Assert - Should never throw, always return Valid result
    extremeCases.foreach { artistName =>
      val result = LastFmDataValidation.validateArtistName(artistName)
      result shouldBe a[Valid[_]]
      val cleanedName = result.asInstanceOf[Valid[String]].value
      cleanedName should not be null
      cleanedName.trim should not be empty
    }
  }

  it should "provide consistent default values" in {
    // Act - Test that default values are consistent
    val nullTrackResult = LastFmDataValidation.validateTrackName(null)
    val emptyTrackResult = LastFmDataValidation.validateTrackName("")
    val blankTrackResult = LastFmDataValidation.validateTrackName("   ")

    val nullArtistResult = LastFmDataValidation.validateArtistName(null)
    val emptyArtistResult = LastFmDataValidation.validateArtistName("")
    val blankArtistResult = LastFmDataValidation.validateArtistName("   ")

    // Assert - Consistent defaults should be used
    nullTrackResult should be(Valid("Unknown Track"))
    emptyTrackResult should be(Valid("Unknown Track"))
    blankTrackResult should be(Valid("Unknown Track"))

    nullArtistResult should be(Valid("Unknown Artist"))
    emptyArtistResult should be(Valid("Unknown Artist"))
    blankArtistResult should be(Valid("Unknown Artist"))
  }
}
