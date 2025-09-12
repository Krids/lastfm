package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test specification for TrackPopularity domain model.
 * 
 * Tests track aggregation statistics and validation.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TrackPopularitySpec extends AnyFlatSpec with Matchers {

  behavior of "TrackPopularity"

  it should "create track popularity with valid statistics" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Bohemian Rhapsody",
      artistName = "Queen",
      playCount = 100,
      uniqueSessions = 50,
      uniqueUsers = 30
    )

    popularity.rank shouldBe 1
    popularity.trackName shouldBe "Bohemian Rhapsody"
    popularity.artistName shouldBe "Queen"
    popularity.playCount shouldBe 100
    popularity.uniqueSessions shouldBe 50
    popularity.uniqueUsers shouldBe 30
  }

  it should "maintain correct play count" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Song",
      artistName = "Artist",
      playCount = 523,
      uniqueSessions = 50,
      uniqueUsers = 30
    )

    popularity.playCount shouldBe 523
  }

  it should "track unique session count accurately" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Song",
      artistName = "Artist",
      playCount = 100,
      uniqueSessions = 45,
      uniqueUsers = 30
    )

    popularity.uniqueSessions shouldBe 45
  }

  it should "track unique user count correctly" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Song",
      artistName = "Artist",
      playCount = 100,
      uniqueSessions = 50,
      uniqueUsers = 25
    )

    popularity.uniqueUsers shouldBe 25
  }

  it should "provide composite track identifier" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Bohemian Rhapsody",
      artistName = "Queen",
      playCount = 100,
      uniqueSessions = 50,
      uniqueUsers = 30
    )

    popularity.compositeKey shouldBe "Queen - Bohemian Rhapsody"
  }

  // Validation Tests
  it should "reject negative rank" in {
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = -1,
        trackName = "Song",
        artistName = "Artist",
        playCount = 100,
        uniqueSessions = 50,
        uniqueUsers = 30
      )
    }
  }

  it should "reject zero rank" in {
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 0,
        trackName = "Song",
        artistName = "Artist",
        playCount = 100,
        uniqueSessions = 50,
        uniqueUsers = 30
      )
    }
  }

  it should "reject null track name" in {
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = null,
        artistName = "Artist",
        playCount = 100,
        uniqueSessions = 50,
        uniqueUsers = 30
      )
    }
  }

  it should "reject empty track name" in {
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "",
        artistName = "Artist",
        playCount = 100,
        uniqueSessions = 50,
        uniqueUsers = 30
      )
    }
  }

  it should "reject null artist name" in {
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Song",
        artistName = null,
        playCount = 100,
        uniqueSessions = 50,
        uniqueUsers = 30
      )
    }
  }

  it should "reject negative play count" in {
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Song",
        artistName = "Artist",
        playCount = -1,
        uniqueSessions = 50,
        uniqueUsers = 30
      )
    }
  }

  it should "reject play count less than session count" in {
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Song",
        artistName = "Artist",
        playCount = 10,
        uniqueSessions = 50, // more sessions than plays - impossible
        uniqueUsers = 30
      )
    }
  }

  it should "reject session count less than user count" in {
    an[IllegalArgumentException] should be thrownBy {
      TrackPopularity(
        rank = 1,
        trackName = "Song",
        artistName = "Artist",
        playCount = 100,
        uniqueSessions = 20,
        uniqueUsers = 30 // more users than sessions - impossible
      )
    }
  }

  it should "accept zero play count with zero sessions and users" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Song",
      artistName = "Artist",
      playCount = 0,
      uniqueSessions = 0,
      uniqueUsers = 0
    )

    popularity.playCount shouldBe 0
  }

  // Edge Cases
  it should "handle tracks with identical names but different artists" in {
    val popularity1 = TrackPopularity(
      rank = 1,
      trackName = "Yesterday",
      artistName = "The Beatles",
      playCount = 100,
      uniqueSessions = 50,
      uniqueUsers = 30
    )

    val popularity2 = TrackPopularity(
      rank = 2,
      trackName = "Yesterday",
      artistName = "Ray Charles",
      playCount = 80,
      uniqueSessions = 40,
      uniqueUsers = 25
    )

    popularity1.compositeKey should not equal popularity2.compositeKey
  }

  it should "handle tracks with special characters" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Song♫Name",
      artistName = "Artist & Band",
      playCount = 100,
      uniqueSessions = 50,
      uniqueUsers = 30
    )

    popularity.trackName should include("♫")
    popularity.artistName should include("&")
  }

  it should "handle very long track names" in {
    val longName = "A" * 500
    val popularity = TrackPopularity(
      rank = 1,
      trackName = longName,
      artistName = "Artist",
      playCount = 100,
      uniqueSessions = 50,
      uniqueUsers = 30
    )

    popularity.trackName.length shouldBe 500
  }

  it should "handle tracks with Unicode in names" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "歌曲",
      artistName = "艺术家",
      playCount = 100,
      uniqueSessions = 50,
      uniqueUsers = 30
    )

    popularity.trackName shouldBe "歌曲"
    popularity.artistName shouldBe "艺术家"
  }

  it should "handle tracks played once by single user" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Rare Song",
      artistName = "Underground Artist",
      playCount = 1,
      uniqueSessions = 1,
      uniqueUsers = 1
    )

    popularity.playCount shouldBe 1
    popularity.uniqueSessions shouldBe 1
    popularity.uniqueUsers shouldBe 1
  }

  it should "handle tracks played multiple times in same session" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Repeat Song",
      artistName = "Artist",
      playCount = 10, // played 10 times
      uniqueSessions = 1, // but all in one session
      uniqueUsers = 1 // by one user
    )

    popularity.playCount shouldBe 10
    popularity.uniqueSessions shouldBe 1
  }

  it should "calculate average plays per session" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Song",
      artistName = "Artist",
      playCount = 100,
      uniqueSessions = 25,
      uniqueUsers = 20
    )

    popularity.averagePlaysPerSession shouldBe 4.0
  }

  it should "calculate average plays per user" in {
    val popularity = TrackPopularity(
      rank = 1,
      trackName = "Song",
      artistName = "Artist",
      playCount = 100,
      uniqueSessions = 50,
      uniqueUsers = 20
    )

    popularity.averagePlaysPerUser shouldBe 5.0
  }
}