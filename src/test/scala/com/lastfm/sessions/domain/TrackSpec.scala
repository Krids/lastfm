package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test specification for Track domain model.
 * 
 * Simple value object representing a music track.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TrackSpec extends AnyFlatSpec with Matchers {

  behavior of "Track"

  it should "create track with valid data" in {
    val track = Track(
      trackId = "track123",
      trackName = "Bohemian Rhapsody",
      artistId = "artist456",
      artistName = "Queen"
    )

    track.trackId shouldBe "track123"
    track.trackName shouldBe "Bohemian Rhapsody"
    track.artistId shouldBe "artist456"
    track.artistName shouldBe "Queen"
  }

  it should "reject null track name" in {
    an[IllegalArgumentException] should be thrownBy {
      Track("track123", null, "artist456", "Queen")
    }
  }

  it should "reject empty track name" in {
    an[IllegalArgumentException] should be thrownBy {
      Track("track123", "", "artist456", "Queen")
    }
  }

  it should "reject null artist name" in {
    an[IllegalArgumentException] should be thrownBy {
      Track("track123", "Bohemian Rhapsody", "artist456", null)
    }
  }

  it should "reject empty artist name" in {
    an[IllegalArgumentException] should be thrownBy {
      Track("track123", "Bohemian Rhapsody", "artist456", "")
    }
  }

  it should "accept null track ID (optional field)" in {
    val track = Track(null, "Bohemian Rhapsody", "artist456", "Queen")
    track.trackId shouldBe null
  }

  it should "accept null artist ID (optional field)" in {
    val track = Track("track123", "Bohemian Rhapsody", null, "Queen")
    track.artistId shouldBe null
  }

  it should "provide composite key for identity" in {
    val track = Track("track123", "Bohemian Rhapsody", "artist456", "Queen")
    track.compositeKey shouldBe "queen - bohemian rhapsody"
  }

  it should "handle special characters in composite key" in {
    val track = Track("track123", "Song/Name", "artist456", "Artist & Band")
    track.compositeKey shouldBe "artist & band - song/name"
  }

  it should "implement equality based on all fields" in {
    val track1 = Track("track123", "Bohemian Rhapsody", "artist456", "Queen")
    val track2 = Track("track123", "Bohemian Rhapsody", "artist456", "Queen")
    val track3 = Track("track999", "Bohemian Rhapsody", "artist456", "Queen")

    track1 shouldEqual track2
    track1 should not equal track3
  }

  it should "implement hashCode consistently with equals" in {
    val track1 = Track("track123", "Bohemian Rhapsody", "artist456", "Queen")
    val track2 = Track("track123", "Bohemian Rhapsody", "artist456", "Queen")

    track1.hashCode shouldEqual track2.hashCode
  }
}

