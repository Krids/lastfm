package com.lastfm.sessions.domain.services

import com.lastfm.sessions.domain._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Test specification for TrackAggregator domain service.
 * 
 * Tests track aggregation and popularity calculation logic.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TrackAggregatorSpec extends AnyFlatSpec with Matchers {

  behavior of "TrackAggregator"

  // Test fixtures
  private def createSessionWithTracks(
    sessionId: String,
    userId: String,
    tracks: List[Track]
  ): RankedSession = {
    RankedSession(
      rank = 1,
      sessionId = sessionId,
      userId = userId,
      trackCount = tracks.size,
      durationMinutes = 60,
      startTime = Instant.parse("2023-01-01T10:00:00Z"),
      endTime = Instant.parse("2023-01-01T11:00:00Z"),
      tracks = tracks
    )
  }

  it should "count total track plays across all sessions" in {
    val track1 = Track("t1", "Song A", "a1", "Artist 1")
    val track2 = Track("t2", "Song B", "a2", "Artist 2")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track1, track2)),
      createSessionWithTracks("s2", "user2", List(track1, track1, track2)),
      createSessionWithTracks("s3", "user3", List(track1))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    val track1Pop = popularity.find(_.trackName == "Song A").get
    val track2Pop = popularity.find(_.trackName == "Song B").get

    track1Pop.playCount shouldBe 4 // 1 + 2 + 1
    track2Pop.playCount shouldBe 2 // 1 + 1
  }

  it should "count unique sessions per track" in {
    val track = Track("t1", "Song A", "a1", "Artist 1")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track, track, track)),
      createSessionWithTracks("s2", "user2", List(track)),
      createSessionWithTracks("s3", "user3", List(track, track))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    val trackPop = popularity.head
    trackPop.playCount shouldBe 6 // 3 + 1 + 2
    trackPop.uniqueSessions shouldBe 3 // Appears in all 3 sessions
  }

  it should "count unique users per track" in {
    val track = Track("t1", "Song A", "a1", "Artist 1")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track)),
      createSessionWithTracks("s2", "user1", List(track)), // Same user
      createSessionWithTracks("s3", "user2", List(track)),
      createSessionWithTracks("s4", "user3", List(track))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    val trackPop = popularity.head
    trackPop.uniqueUsers shouldBe 3 // user1, user2, user3
  }

  it should "handle same track in multiple sessions" in {
    val track = Track("t1", "Popular Song", "a1", "Famous Artist")
    val otherTrack = Track("t2", "Other Song", "a2", "Other Artist")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track, otherTrack)),
      createSessionWithTracks("s2", "user2", List(track)),
      createSessionWithTracks("s3", "user3", List(track, track)),
      createSessionWithTracks("s4", "user4", List(otherTrack))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    val popularTrack = popularity.find(_.trackName == "Popular Song").get
    popularTrack.playCount shouldBe 4
    popularTrack.uniqueSessions shouldBe 3
    popularTrack.uniqueUsers shouldBe 3
  }

  it should "handle same track multiple times in one session" in {
    val track = Track("t1", "Repeat Song", "a1", "Artist")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track, track, track, track, track))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    val trackPop = popularity.head
    trackPop.playCount shouldBe 5
    trackPop.uniqueSessions shouldBe 1
    trackPop.uniqueUsers shouldBe 1
  }

  it should "group tracks by artist and name combination" in {
    // Same song name, different artists - should be different tracks
    val track1 = Track("t1", "Yesterday", "a1", "The Beatles")
    val track2 = Track("t2", "Yesterday", "a2", "Ray Charles")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track1, track2))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    popularity.size shouldBe 2
    popularity.map(t => s"${t.artistName} - ${t.trackName}").sorted shouldBe List(
      "Ray Charles - Yesterday",
      "The Beatles - Yesterday"
    )
  }

  it should "treat same track with different case as identical" in {
    val track1 = Track("t1", "bohemian rhapsody", "a1", "queen")
    val track2 = Track("t2", "Bohemian Rhapsody", "a2", "Queen")
    val track3 = Track("t3", "BOHEMIAN RHAPSODY", "a3", "QUEEN")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track1)),
      createSessionWithTracks("s2", "user2", List(track2)),
      createSessionWithTracks("s3", "user3", List(track3))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    popularity.size shouldBe 1
    val trackPop = popularity.head
    trackPop.playCount shouldBe 3
    trackPop.uniqueSessions shouldBe 3
  }

  it should "distinguish tracks with same name but different artists" in {
    val beatlesVersion = Track("t1", "Come Together", "a1", "The Beatles")
    val aerosmithVersion = Track("t2", "Come Together", "a2", "Aerosmith")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(beatlesVersion, beatlesVersion)),
      createSessionWithTracks("s2", "user2", List(aerosmithVersion))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    popularity.size shouldBe 2
    val beatles = popularity.find(_.artistName == "The Beatles").get
    val aerosmith = popularity.find(_.artistName == "Aerosmith").get
    
    beatles.playCount shouldBe 2
    aerosmith.playCount shouldBe 1
  }

  it should "handle tracks with special characters in names" in {
    val track1 = Track("t1", "Song & Name", "a1", "Artist & Band")
    val track2 = Track("t2", "Song/Name", "a2", "Artist/Band")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track1, track2))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    popularity.size shouldBe 2
    popularity.map(_.trackName) should contain allOf("Song & Name", "Song/Name")
  }

  it should "normalize whitespace in track identifiers" in {
    val track1 = Track("t1", "  Song  Name  ", "a1", "  Artist  Name  ")
    val track2 = Track("t2", "Song Name", "a2", "Artist Name")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track1)),
      createSessionWithTracks("s2", "user2", List(track2))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    popularity.size shouldBe 1 // Should be treated as same track
    popularity.head.playCount shouldBe 2
  }

  it should "handle empty sessions collection" in {
    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(List.empty)

    popularity shouldBe empty
  }

  it should "handle sessions with single track" in {
    val track = Track("t1", "Only Song", "a1", "Only Artist")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track))
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    popularity.size shouldBe 1
    popularity.head.playCount shouldBe 1
  }

  it should "handle sessions with all unique tracks" in {
    val tracks = (1 to 5).map { i =>
      Track(s"t$i", s"Song $i", s"a$i", s"Artist $i")
    }.toList
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", tracks)
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    popularity.size shouldBe 5
    popularity.foreach { track =>
      track.playCount shouldBe 1
      track.uniqueSessions shouldBe 1
      track.uniqueUsers shouldBe 1
    }
  }

  it should "handle sessions with all duplicate tracks" in {
    val track = Track("t1", "Same Song", "a1", "Same Artist")
    val duplicates = List.fill(10)(track)
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", duplicates)
    )

    val aggregator = new TrackAggregator()
    val popularity = aggregator.aggregateTrackPopularity(sessions)

    popularity.size shouldBe 1
    popularity.head.playCount shouldBe 10
  }

  it should "handle very large track collections efficiently" in {
    val largeSessions = (1 to 100).map { i =>
      val tracks = (1 to 100).map { j =>
        Track(s"t${i}_$j", s"Song $j", s"a$j", s"Artist $j")
      }.toList
      createSessionWithTracks(s"s$i", s"user$i", tracks)
    }.toList

    val aggregator = new TrackAggregator()
    val start = System.currentTimeMillis()
    val popularity = aggregator.aggregateTrackPopularity(largeSessions)
    val duration = System.currentTimeMillis() - start

    popularity.size shouldBe 100 // 100 unique songs
    duration should be < 1000L // Should complete in less than 1 second
  }

  it should "calculate aggregation statistics correctly" in {
    val track1 = Track("t1", "Popular", "a1", "Artist")
    val track2 = Track("t2", "Less Popular", "a2", "Artist")
    
    val sessions = List(
      createSessionWithTracks("s1", "user1", List(track1, track1, track2)),
      createSessionWithTracks("s2", "user2", List(track1))
    )

    val aggregator = new TrackAggregator()
    val stats = aggregator.calculateAggregationStatistics(sessions)

    stats.totalTracks shouldBe 4
    stats.uniqueTracks shouldBe 2
    stats.totalSessions shouldBe 2
    stats.averageTracksPerSession shouldBe 2.0
  }
}