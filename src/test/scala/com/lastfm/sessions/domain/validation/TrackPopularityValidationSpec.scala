package com.lastfm.sessions.domain.validation

import com.lastfm.sessions.domain.TrackPopularity
import com.lastfm.sessions.utils.BaseTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Comprehensive validation test specification for TrackPopularity domain model.
 * 
 * Tests all constructor validation rules and business logic to achieve complete
 * branch coverage of the TrackPopularity class validation and business logic.
 * 
 * Coverage Target: All 9 require statements + business logic branches
 * Expected Impact: +6% statement coverage, +4% branch coverage
 * 
 * Follows TDD principles with comprehensive edge case testing for ranking scenarios.
 * Uses BaseTestSpec for complete test isolation and safety.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TrackPopularityValidationSpec extends AnyFlatSpec with BaseTestSpec with Matchers {

  /**
   * Tests for constructor validation rules (9+ require statements).
   * Each test targets specific validation rules to ensure complete coverage.
   */
  "TrackPopularity constructor validation" should "reject zero or negative rank" in {
    // Act & Assert - Test require(rank > 0) boundary
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 0,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }

    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = -1,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject null track name" in {
    // Act & Assert - Test require(trackName != null && trackName.trim.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = null,
        artistName = "Valid Artist",
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject empty track name" in {
    // Act & Assert - Test trackName.trim.nonEmpty validation
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "",
        artistName = "Valid Artist",
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject blank track name" in {
    // Act & Assert - Test trackName.trim.nonEmpty with whitespace
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "   ",
        artistName = "Valid Artist",
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject null artist name" in {
    // Act & Assert - Test require(artistName != null && artistName.trim.nonEmpty)
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = null,
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject empty artist name" in {
    // Act & Assert - Test artistName.trim.nonEmpty validation
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "",
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject blank artist name" in {
    // Act & Assert - Test artistName.trim.nonEmpty with whitespace
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "   ",
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject negative play count" in {
    // Act & Assert - Test require(playCount >= 0)
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = -1,
        uniqueSessions = 1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject negative unique sessions" in {
    // Act & Assert - Test require(uniqueSessions >= 0)
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = 1,
        uniqueSessions = -1,
        uniqueUsers = 1
      )
    }
  }

  it should "reject negative unique users" in {
    // Act & Assert - Test require(uniqueUsers >= 0)
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = 1,
        uniqueSessions = 1,
        uniqueUsers = -1
      )
    }
  }

  /**
   * Tests for logical consistency validation rules.
   * These test complex require statements with multiple conditions.
   */
  "TrackPopularity count relationship validation" should "reject playCount less than uniqueSessions" in {
    // Act & Assert - Test require(playCount >= uniqueSessions)
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = 5,
        uniqueSessions = 10, // Cannot have more sessions than plays
        uniqueUsers = 1
      )
    }
  }

  it should "reject uniqueSessions less than uniqueUsers" in {
    // Act & Assert - Test require(uniqueSessions >= uniqueUsers)
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = 10,
        uniqueSessions = 5,
        uniqueUsers = 8 // Cannot have more users than sessions
      )
    }
  }

  it should "enforce zero play count consistency rules" in {
    // Act & Assert - Test special case: if (playCount == 0) require(uniqueSessions == 0 && uniqueUsers == 0)
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = 0,
        uniqueSessions = 1, // Violates zero consistency
        uniqueUsers = 0
      )
    }

    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Valid Track",
        artistName = "Valid Artist",
        playCount = 0,
        uniqueSessions = 0,
        uniqueUsers = 1 // Violates zero consistency
      )
    }
  }

  it should "accept valid zero play count scenario" in {
    // Arrange & Act - Valid zero case should pass all validations
    val validZeroTrack = TrackPopularity(
      rank = 1,
      trackName = "Unplayed Track",
      artistName = "Unknown Artist",
      playCount = 0,
      uniqueSessions = 0,
      uniqueUsers = 0
    )

    // Assert - Should create successfully and maintain consistency
    validZeroTrack.playCount should be(0)
    validZeroTrack.uniqueSessions should be(0)
    validZeroTrack.uniqueUsers should be(0)
    validZeroTrack.rank should be(1)
  }

  /**
   * Tests for business logic edge cases and realistic scenarios.
   */
  "TrackPopularity realistic ranking scenarios" should "handle typical popular track statistics" in {
    // Arrange - Realistic popular track data
    val popularTrack = TrackPopularity(
      rank = 1,
      trackName = "Bohemian Rhapsody",
      artistName = "Queen",
      playCount = 523,
      uniqueSessions = 450,
      uniqueUsers = 380
    )

    // Act & Assert - Should handle realistic values correctly
    popularTrack.rank should be(1)
    popularTrack.playCount should be(523)
    popularTrack.uniqueSessions should be(450)
    popularTrack.uniqueUsers should be(380)
  }

  it should "handle niche track with low statistics" in {
    // Arrange - Track played once by single user
    val nicheTrack = TrackPopularity(
      rank = 100,
      trackName = "Obscure B-Side",
      artistName = "Indie Artist",
      playCount = 1,
      uniqueSessions = 1,
      uniqueUsers = 1
    )

    // Act & Assert - Should handle minimal valid values
    nicheTrack.playCount should be(1)
    nicheTrack.uniqueSessions should be(1)
    nicheTrack.uniqueUsers should be(1)
  }

  it should "handle track with multiple plays in same session" in {
    // Arrange - Track played multiple times by same user in same session
    val repeatedTrack = TrackPopularity(
      rank = 5,
      trackName = "On Repeat",
      artistName = "Favorite Artist",
      playCount = 10,
      uniqueSessions = 2, // Played in 2 sessions
      uniqueUsers = 1     // But only 1 user
    )

    // Act & Assert - Should handle repeat listening patterns
    repeatedTrack.playCount should be(10)
    repeatedTrack.uniqueSessions should be(2)
    repeatedTrack.uniqueUsers should be(1)
  }

  it should "handle track with special characters in names" in {
    // Arrange - Track and artist names with special characters
    val specialCharTrack = TrackPopularity(
      rank = 10,
      trackName = "Track/Name (Remix) - Extended Version",
      artistName = "Artist & The Band feat. Guest",
      playCount = 25,
      uniqueSessions = 20,
      uniqueUsers = 15
    )

    // Act & Assert - Should handle special characters correctly
    specialCharTrack.trackName should include("/")
    specialCharTrack.trackName should include("(")
    specialCharTrack.trackName should include(")")
    specialCharTrack.trackName should include("-")
    specialCharTrack.artistName should include("&")
    specialCharTrack.artistName should include("feat.")
  }

  it should "handle Unicode track and artist names" in {
    // Arrange - International track names (Japanese, Norwegian)
    val unicodeTrack = TrackPopularity(
      rank = 7,
      trackName = "トラック名",
      artistName = "坂本龍一",
      playCount = 45,
      uniqueSessions = 40,
      uniqueUsers = 35
    )

    val nordicTrack = TrackPopularity(
      rank = 8,
      trackName = "Sámi Song",
      artistName = "Sigur Rós",
      playCount = 42,
      uniqueSessions = 38,
      uniqueUsers = 33
    )

    // Act & Assert - Should preserve Unicode characters correctly
    unicodeTrack.trackName should be("トラック名")
    unicodeTrack.artistName should be("坂本龍一")

    nordicTrack.trackName should be("Sámi Song")
    nordicTrack.artistName should be("Sigur Rós")
  }

  /**
   * Tests for boundary value scenarios and extreme cases.
   */
  "TrackPopularity boundary scenarios" should "handle maximum rank values" in {
    // Arrange - Test with very high rank positions
    val highRankTrack = TrackPopularity(
      rank = Int.MaxValue,
      trackName = "Least Popular",
      artistName = "Unknown Artist",
      playCount = 1,
      uniqueSessions = 1,
      uniqueUsers = 1
    )

    // Act & Assert - Should handle maximum rank values
    highRankTrack.rank should be(Int.MaxValue)
  }

  it should "handle maximum play count values" in {
    // Arrange - Test with very high play counts
    val viralTrack = TrackPopularity(
      rank = 1,
      trackName = "Viral Hit",
      artistName = "Chart Topper",
      playCount = Int.MaxValue,
      uniqueSessions = 1000000,
      uniqueUsers = 500000
    )

    // Act & Assert - Should handle maximum values without overflow
    viralTrack.playCount should be(Int.MaxValue)
    viralTrack.uniqueSessions should be(1000000)
    viralTrack.uniqueUsers should be(500000)
  }

  it should "handle perfect relationship boundaries" in {
    // Arrange - Test exact boundary conditions for count relationships
    val perfectBoundary = TrackPopularity(
      rank = 1,
      trackName = "Boundary Track",
      artistName = "Boundary Artist",
      playCount = 100,
      uniqueSessions = 100, // Exact equality: playCount == uniqueSessions
      uniqueUsers = 100     // Exact equality: uniqueSessions == uniqueUsers
    )

    // Act & Assert - Should accept exact equality in relationships
    perfectBoundary.playCount should be(100)
    perfectBoundary.uniqueSessions should be(100)
    perfectBoundary.uniqueUsers should be(100)
  }

  /**
   * Tests for business logic methods and computed properties.
   */
  "TrackPopularity business logic" should "generate consistent composite keys" in {
    // Arrange - Track with standard naming
    val standardTrack = TrackPopularity(
      rank = 1,
      trackName = "Standard Track Name",
      artistName = "Standard Artist",
      playCount = 10,
      uniqueSessions = 8,
      uniqueUsers = 6
    )

    val specialCharTrack = TrackPopularity(
      rank = 2,
      trackName = "Track/With#Special!Characters",
      artistName = "Artist & Band (feat. Guest)",
      playCount = 5,
      uniqueSessions = 4,
      uniqueUsers = 3
    )

    // Act & Assert - Should generate deterministic composite keys
    val key1 = standardTrack.compositeKey
    val key2 = specialCharTrack.compositeKey

    key1 should include("Standard Artist")
    key1 should include("Standard Track Name")

    key2 should include("Artist & Band (feat. Guest)")
    key2 should include("Track/With#Special!Characters")

    // Keys should be deterministic for same input
    standardTrack.compositeKey should be(standardTrack.compositeKey)
    specialCharTrack.compositeKey should be(specialCharTrack.compositeKey)
  }

  it should "handle edge case count relationships correctly" in {
    // Arrange - Various valid count relationship scenarios
    val singleUserMultipleSessions = TrackPopularity(
      rank = 1,
      trackName = "Repeated Track",
      artistName = "Favorite Artist",
      playCount = 50,
      uniqueSessions = 10, // Same user, multiple sessions
      uniqueUsers = 1
    )

    val multipleUsersOneSession = TrackPopularity(
      rank = 2,
      trackName = "Party Track",
      artistName = "Party Band",
      playCount = 20,
      uniqueSessions = 20, // Each user played once
      uniqueUsers = 20
    )

    val complexScenario = TrackPopularity(
      rank = 3,
      trackName = "Complex Track",
      artistName = "Popular Artist",
      playCount = 1000,
      uniqueSessions = 750, // Some repeat plays within sessions
      uniqueUsers = 500     // Some users played in multiple sessions
    )

    // Act & Assert - All scenarios should be valid and maintain relationships
    singleUserMultipleSessions.playCount should be >= singleUserMultipleSessions.uniqueSessions
    singleUserMultipleSessions.uniqueSessions should be >= singleUserMultipleSessions.uniqueUsers

    multipleUsersOneSession.playCount should be >= multipleUsersOneSession.uniqueSessions
    multipleUsersOneSession.uniqueSessions should be >= multipleUsersOneSession.uniqueUsers

    complexScenario.playCount should be >= complexScenario.uniqueSessions
    complexScenario.uniqueSessions should be >= complexScenario.uniqueUsers
  }

  /**
   * Tests for realistic music industry scenarios and data patterns.
   */
  "TrackPopularity music industry scenarios" should "handle classical music naming conventions" in {
    // Arrange - Classical music with complex naming
    val classicalTrack = TrackPopularity(
      rank = 15,
      trackName = "Symphony No. 9 in D Minor, Op. 125 \"Choral\": IV. Presto - Allegro assai",
      artistName = "Ludwig van Beethoven",
      playCount = 150,
      uniqueSessions = 120,
      uniqueUsers = 90
    )

    // Act & Assert - Should handle long, complex classical names
    classicalTrack.trackName.length should be > 50
    classicalTrack.trackName should include("Symphony")
    classicalTrack.trackName should include("Choral")
    classicalTrack.artistName should include("Ludwig van Beethoven")
  }

  it should "handle modern track naming with features and remixes" in {
    // Arrange - Modern track with featuring artists and remix info
    val modernTrack = TrackPopularity(
      rank = 3,
      trackName = "Song Title (Remix) [Radio Edit]",
      artistName = "Main Artist feat. Guest Artist & Another Artist",
      playCount = 800,
      uniqueSessions = 650,
      uniqueUsers = 500
    )

    // Act & Assert - Should handle modern naming conventions
    modernTrack.trackName should include("Remix")
    modernTrack.trackName should include("Radio Edit")
    modernTrack.artistName should include("feat.")
    modernTrack.artistName should include("&")
  }

  it should "handle extremely popular tracks (viral scenarios)" in {
    // Arrange - Viral track with extreme popularity
    val viralTrack = TrackPopularity(
      rank = 1,
      trackName = "Viral Hit of the Year",
      artistName = "Chart Dominator",
      playCount = 1000000,
      uniqueSessions = 800000,
      uniqueUsers = 600000
    )

    // Act & Assert - Should handle extreme popularity values
    viralTrack.playCount should be(1000000)
    viralTrack.uniqueSessions should be(800000)
    viralTrack.uniqueUsers should be(600000)
    viralTrack.rank should be(1)
  }

  it should "handle niche tracks with minimal plays" in {
    // Arrange - Niche track played minimally
    val nicheTrack = TrackPopularity(
      rank = 9999,
      trackName = "Deep Album Cut",
      artistName = "Obscure Indie Band",
      playCount = 1,
      uniqueSessions = 1,
      uniqueUsers = 1
    )

    // Act & Assert - Should handle minimal valid statistics
    nicheTrack.playCount should be(1)
    nicheTrack.uniqueSessions should be(1)
    nicheTrack.uniqueUsers should be(1)
    nicheTrack.rank should be(9999)
  }

  /**
   * Tests for data quality edge cases in real-world music data.
   */
  "TrackPopularity data quality edge cases" should "handle tracks with podcast-like content" in {
    // Arrange - Content that might be podcasts rather than music
    val podcastLikeTrack = TrackPopularity(
      rank = 50,
      trackName = "Episode 123: Interview with Guest",
      artistName = "Podcast Show Name",
      playCount = 75,
      uniqueSessions = 75, // Each play in separate session (typical podcast behavior)
      uniqueUsers = 75     // Each user listens once
    )

    // Act & Assert - Should handle podcast-like listening patterns
    podcastLikeTrack.playCount should be(75)
    podcastLikeTrack.uniqueSessions should be(75)
    podcastLikeTrack.uniqueUsers should be(75)
  }

  it should "handle live performance vs studio recording variants" in {
    // Arrange - Same song, different versions
    val studioVersion = TrackPopularity(
      rank = 10,
      trackName = "Song Name (Studio Version)",
      artistName = "Recording Artist",
      playCount = 200,
      uniqueSessions = 180,
      uniqueUsers = 150
    )

    val liveVersion = TrackPopularity(
      rank = 25,
      trackName = "Song Name (Live at Venue)",
      artistName = "Recording Artist",
      playCount = 80,
      uniqueSessions = 75,
      uniqueUsers = 65
    )

    // Act & Assert - Should distinguish between versions
    studioVersion.trackName should include("Studio")
    liveVersion.trackName should include("Live")
    studioVersion.artistName should be(liveVersion.artistName) // Same artist
    studioVersion.compositeKey should not be liveVersion.compositeKey // Different composite keys
  }

  it should "handle compilation and various artists scenarios" in {
    // Arrange - Track from compilation album
    val compilationTrack = TrackPopularity(
      rank = 30,
      trackName = "Classic Hit",
      artistName = "Various Artists",
      playCount = 120,
      uniqueSessions = 100,
      uniqueUsers = 80
    )

    // Act & Assert - Should handle compilation scenarios
    compilationTrack.artistName should be("Various Artists")
    compilationTrack.compositeKey should include("Various Artists")
    compilationTrack.compositeKey should include("Classic Hit")
  }
}
