package com.lastfm.sessions.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.lastfm.sessions.common.traits.DataValidator
import com.lastfm.sessions.domain.{Valid, Invalid}

/**
 * Tests for basic validation functionality.
 * 
 * Simplified validation tests focusing on core functionality.
 */
class ValidationBasicsSpec extends AnyFlatSpec with Matchers with DataValidator {
  
  "DataValidator" should "validate user IDs correctly" in {
    validateUserId("user_000001") should be(Valid("user_000001"))
    validateUserId("user_999999") should be(Valid("user_999999"))
    
    validateUserId("invalid") shouldBe an[Invalid]
    validateUserId("user_abc") shouldBe an[Invalid]
    validateUserId("USER_000001") shouldBe an[Invalid] // Case sensitive
    validateUserId(null) shouldBe an[Invalid]
    validateUserId("") shouldBe an[Invalid]
  }
  
  it should "validate timestamps correctly" in {
    val validTimestamp = "2023-01-01T10:00:00Z"
    validateTimestamp(validTimestamp) shouldBe a[Valid[_]]
    
    validateTimestamp("invalid-timestamp") shouldBe an[Invalid]
    validateTimestamp("2023-13-45T25:99:99Z") shouldBe an[Invalid] // Invalid date
    validateTimestamp(null) shouldBe an[Invalid]
    validateTimestamp("") shouldBe an[Invalid]
  }
  
  it should "validate track names correctly" in {
    validateTrackName("Valid Track") should be("Valid Track")
    validateTrackName("") should be("Unknown Track")
    validateTrackName(null) should be("Unknown Track")
    validateTrackName("   ") should be("Unknown Track")
    
    // Long track name should be truncated
    val longTrackName = "x" * 1000
    val result = validateTrackName(longTrackName)
    result.length should be <= Constants.Limits.MAX_TRACK_NAME_LENGTH
  }
  
  it should "validate artist names correctly" in {
    validateArtistName("Valid Artist") should be("Valid Artist")
    validateArtistName("") should be("Unknown Artist")
    validateArtistName(null) should be("Unknown Artist")
    
    // Should handle Unicode
    validateArtistName("坂本龍一") should be("坂本龍一")
    validateArtistName("Sigur Rós") should be("Sigur Rós")
  }
  
  it should "generate track keys correctly" in {
    // With track ID
    generateTrackKey(Some("track-123"), "Artist", "Track") should be("track-123")
    
    // Without track ID
    generateTrackKey(None, "Artist", "Track") should be(s"Artist${Constants.DataPatterns.TRACK_KEY_SEPARATOR}Track")
    
    // With empty track ID
    generateTrackKey(Some(""), "Artist", "Track") should be(s"Artist${Constants.DataPatterns.TRACK_KEY_SEPARATOR}Track")
  }
  
  it should "validate session gaps correctly" in {
    validateSessionGap(20) should be(Valid(20))
    validateSessionGap(1) should be(Valid(1))
    validateSessionGap(1440) should be(Valid(1440)) // 24 hours max
    
    validateSessionGap(0) shouldBe an[Invalid]
    validateSessionGap(-1) shouldBe an[Invalid]
    validateSessionGap(1500) shouldBe an[Invalid] // > 24 hours
  }
  
  it should "validate quality scores correctly" in {
    validateQualityScore(99.5) should be(Valid(99.5))
    validateQualityScore(0.0) should be(Valid(0.0))
    validateQualityScore(100.0) should be(Valid(100.0))
    
    validateQualityScore(-1.0) shouldBe an[Invalid]
    validateQualityScore(101.0) shouldBe an[Invalid]
  }
  
  it should "validate file paths correctly" in {
    validateFilePath("valid/path", mustExist = false) should be(Valid("valid/path"))
    validateFilePath("/absolute/path", mustExist = false) should be(Valid("/absolute/path"))
    
    validateFilePath(null) shouldBe an[Invalid]
    validateFilePath("") shouldBe an[Invalid]
    validateFilePath("../dangerous/path") shouldBe an[Invalid] // Path traversal
  }
  
  it should "handle edge cases gracefully" in {
    // Very long strings
    val veryLongString = "x" * 1000000
    validateTrackName(veryLongString).length should be <= Constants.Limits.MAX_TRACK_NAME_LENGTH
    
    // Unicode content
    validateTrackName("🎵 Test Track 🎵") should include("🎵")
    validateArtistName("Мумий Тролль") should be("Мумий Тролль")
    
    // Whitespace handling
    validateTrackName("  Track  ") should be("Track")
    validateArtistName("\t\nArtist\r\n") should be("Artist")
  }
}