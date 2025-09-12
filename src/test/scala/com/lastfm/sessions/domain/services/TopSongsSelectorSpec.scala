package com.lastfm.sessions.domain.services

import com.lastfm.sessions.domain._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test specification for TopSongsSelector domain service.
 * 
 * Tests final track selection and ranking logic.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TopSongsSelectorSpec extends AnyFlatSpec with Matchers {

  behavior of "TopSongsSelector"

  // Test fixtures
  private def createTrackPopularity(
    trackName: String,
    artistName: String,
    playCount: Int,
    uniqueSessions: Int,
    uniqueUsers: Int
  ): TrackPopularity = {
    TrackPopularity(
      rank = 1, // Will be reassigned by selector
      trackName = trackName,
      artistName = artistName,
      playCount = playCount,
      uniqueSessions = uniqueSessions,
      uniqueUsers = uniqueUsers
    )
  }

  it should "select tracks with highest play counts" in {
    val tracks = List(
      createTrackPopularity("Song A", "Artist 1", 100, 50, 30),
      createTrackPopularity("Song B", "Artist 2", 200, 60, 40),
      createTrackPopularity("Song C", "Artist 3", 50, 25, 15),
      createTrackPopularity("Song D", "Artist 4", 150, 55, 35)
    )

    val selector = new TopSongsSelector()
    val top3 = selector.selectTopSongs(tracks, 3)

    top3.map(_.trackName) shouldBe List("Song B", "Song D", "Song A")
    top3.map(_.playCount) shouldBe List(200, 150, 100)
  }

  it should "return exactly N tracks when available" in {
    val tracks = (1 to 20).map { i =>
      createTrackPopularity(s"Song $i", s"Artist $i", 100 - i, 50, 30)
    }.toList

    val selector = new TopSongsSelector()
    val top10 = selector.selectTopSongs(tracks, 10)

    top10.size shouldBe 10
    top10.head.playCount shouldBe 99
    top10.last.playCount shouldBe 90
  }

  it should "return fewer tracks when insufficient data" in {
    val tracks = List(
      createTrackPopularity("Song A", "Artist 1", 100, 50, 30),
      createTrackPopularity("Song B", "Artist 2", 200, 60, 40)
    )

    val selector = new TopSongsSelector()
    val top10 = selector.selectTopSongs(tracks, 10)

    top10.size shouldBe 2
  }

  it should "use session count as tiebreaker" in {
    val tracks = List(
      createTrackPopularity("Song A", "Artist 1", 100, 30, 20),
      createTrackPopularity("Song B", "Artist 2", 100, 50, 20),
      createTrackPopularity("Song C", "Artist 3", 100, 40, 20)
    )

    val selector = new TopSongsSelector()
    val ranked = selector.selectTopSongs(tracks, 3)

    ranked.map(_.trackName) shouldBe List("Song B", "Song C", "Song A")
    ranked.map(_.uniqueSessions) shouldBe List(50, 40, 30)
  }

  it should "use user count as secondary tiebreaker" in {
    val tracks = List(
      createTrackPopularity("Song A", "Artist 1", 100, 50, 20),
      createTrackPopularity("Song B", "Artist 2", 100, 50, 30),
      createTrackPopularity("Song C", "Artist 3", 100, 50, 25)
    )

    val selector = new TopSongsSelector()
    val ranked = selector.selectTopSongs(tracks, 3)

    ranked.map(_.trackName) shouldBe List("Song B", "Song C", "Song A")
    ranked.map(_.uniqueUsers) shouldBe List(30, 25, 20)
  }

  it should "use track name for stable ordering" in {
    val tracks = List(
      createTrackPopularity("Zebra", "Artist", 100, 50, 30),
      createTrackPopularity("Apple", "Artist", 100, 50, 30),
      createTrackPopularity("Mango", "Artist", 100, 50, 30)
    )

    val selector = new TopSongsSelector()
    val ranked = selector.selectTopSongs(tracks, 3)

    ranked.map(_.trackName) shouldBe List("Apple", "Mango", "Zebra")
  }

  it should "assign sequential ranking positions" in {
    val tracks = (1 to 5).map { i =>
      createTrackPopularity(s"Song $i", s"Artist $i", 100 - i * 10, 50, 30)
    }.toList

    val selector = new TopSongsSelector()
    val ranked = selector.selectTopSongs(tracks, 5)

    ranked.map(_.rank) shouldBe List(1, 2, 3, 4, 5)
  }

  it should "handle ties in play count consistently" in {
    val tracks = List(
      createTrackPopularity("Song A", "Artist 1", 100, 50, 30),
      createTrackPopularity("Song B", "Artist 2", 100, 50, 30),
      createTrackPopularity("Song C", "Artist 3", 100, 50, 30)
    )

    val selector = new TopSongsSelector()
    val run1 = selector.selectTopSongs(tracks, 3).map(_.trackName)
    val run2 = selector.selectTopSongs(tracks, 3).map(_.trackName)

    run1 shouldEqual run2 // Deterministic
  }

  it should "maintain stable ordering across runs" in {
    val tracks = (1 to 10).map { i =>
      val playCount = 100 + (i % 3) * 50
      val sessions = Math.min(playCount, i * 5)
      val users = Math.min(sessions, i * 3)
      createTrackPopularity(s"Song $i", s"Artist $i", playCount, sessions, users)
    }.toList

    val selector = new TopSongsSelector()
    val run1 = selector.selectTopSongs(tracks, 5)
    val run2 = selector.selectTopSongs(tracks, 5)
    val run3 = selector.selectTopSongs(tracks, 5)

    run1.map(_.trackName) shouldEqual run2.map(_.trackName)
    run2.map(_.trackName) shouldEqual run3.map(_.trackName)
  }

  it should "preserve all track metadata in results" in {
    val track = createTrackPopularity("Song", "Artist", 100, 50, 30)
    val tracks = List(track)

    val selector = new TopSongsSelector()
    val result = selector.selectTopSongs(tracks, 1).head

    result.trackName shouldBe "Song"
    result.artistName shouldBe "Artist"
    result.playCount shouldBe 100
    result.uniqueSessions shouldBe 50
    result.uniqueUsers shouldBe 30
  }

  it should "handle request for zero tracks" in {
    val tracks = List(
      createTrackPopularity("Song", "Artist", 100, 50, 30)
    )

    val selector = new TopSongsSelector()
    val top0 = selector.selectTopSongs(tracks, 0)

    top0 shouldBe empty
  }

  it should "handle empty track collection" in {
    val selector = new TopSongsSelector()
    val result = selector.selectTopSongs(List.empty, 10)

    result shouldBe empty
  }

  it should "handle all tracks with same play count" in {
    val tracks = (1 to 5).map { i =>
      createTrackPopularity(s"Song $i", s"Artist $i", 100, 50 - i, 30)
    }.toList

    val selector = new TopSongsSelector()
    val ranked = selector.selectTopSongs(tracks, 5)

    ranked.foreach(_.playCount shouldBe 100)
    // Should be sorted by session count descending
    ranked.map(_.uniqueSessions) shouldBe List(49, 48, 47, 46, 45)
  }

  it should "handle request for more tracks than available" in {
    val tracks = List(
      createTrackPopularity("Only Song", "Only Artist", 100, 50, 30)
    )

    val selector = new TopSongsSelector()
    val top10 = selector.selectTopSongs(tracks, 10)

    top10.size shouldBe 1
  }

  it should "handle tracks with very high play counts" in {
    val tracks = List(
      createTrackPopularity("Viral Hit", "Famous Artist", Int.MaxValue / 2, 10000, 5000),
      createTrackPopularity("Normal Song", "Normal Artist", 100, 50, 30)
    )

    val selector = new TopSongsSelector()
    val ranked = selector.selectTopSongs(tracks, 2)

    ranked.head.trackName shouldBe "Viral Hit"
    ranked.head.playCount shouldBe (Int.MaxValue / 2)
  }

  it should "validate selection criteria" in {
    val tracks = List(
      createTrackPopularity("Low Play", "Artist", 100, 100, 80),  // Lower plays, max sessions
      createTrackPopularity("High Play", "Artist", 200, 20, 10)   // Higher plays, low sessions
    )

    val selector = new TopSongsSelector()
    val ranked = selector.selectTopSongs(tracks, 2)

    // Play count takes precedence over session count
    ranked.head.trackName shouldBe "High Play"
  }

  it should "calculate selection statistics" in {
    val tracks = List(
      createTrackPopularity("Song A", "Artist 1", 100, 50, 30),
      createTrackPopularity("Song B", "Artist 2", 200, 60, 40),
      createTrackPopularity("Song C", "Artist 3", 50, 25, 15)
    )

    val selector = new TopSongsSelector()
    val stats = selector.calculateSelectionStatistics(tracks)

    stats.totalTracks shouldBe 3
    stats.totalPlayCount shouldBe 350
    stats.averagePlayCount shouldBe (350.0 / 3)
    stats.maxPlayCount shouldBe 200
    stats.minPlayCount shouldBe 50
  }
}