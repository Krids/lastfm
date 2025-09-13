package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Test specification for RankedSession domain model.
 * 
 * Following TDD principles to define behavior before implementation.
 * Tests cover validation, immutability, and business logic.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class RankedSessionSpec extends AnyFlatSpec with Matchers {

  behavior of "RankedSession"

  // Test data fixtures
  private val validSessionId = "user_000001_session_1"
  private val validUserId = "user_000001"
  private val validTracks = List(
    Track("track1", "Track One", "artist1", "Artist One"),
    Track("track2", "Track Two", "artist2", "Artist Two"),
    Track("track3", "Track Three", "artist1", "Artist One")
  )
  private val validStartTime = Instant.parse("2023-01-01T10:00:00Z")
  private val validEndTime = Instant.parse("2023-01-01T11:30:00Z")

  // Happy Path Tests
  it should "create ranked session with valid data" in {
    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 3,
      durationMinutes = 90,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = validTracks
    )

    session.rank shouldBe 1
    session.sessionId shouldBe validSessionId
    session.userId shouldBe validUserId
    session.trackCount shouldBe 3
    session.durationMinutes shouldBe 90
    session.tracks shouldBe validTracks
  }

  it should "maintain immutability after creation" in {
    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 3,
      durationMinutes = 90,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = validTracks
    )

    // Verify all fields are vals (compile-time check)
    session.rank shouldBe 1
    session.tracks.size shouldBe 3
  }

  it should "provide accurate track count" in {
    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 3,
      durationMinutes = 90,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = validTracks
    )

    session.trackCount shouldBe session.tracks.size
  }

  it should "calculate unique track count correctly" in {
    val tracksWithDuplicates = List(
      Track("track1", "Track One", "artist1", "Artist One"),
      Track("track1", "Track One", "artist1", "Artist One"), // duplicate
      Track("track2", "Track Two", "artist2", "Artist Two")
    )

    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 3,
      durationMinutes = 90,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = tracksWithDuplicates
    )

    session.uniqueTrackCount shouldBe 2
  }

  // Validation Tests
  it should "reject negative ranking position" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = -1,
        sessionId = validSessionId,
        userId = validUserId,
        trackCount = 3,
        durationMinutes = 90,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = validTracks
      )
    }
  }

  it should "reject zero ranking position" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 0,
        sessionId = validSessionId,
        userId = validUserId,
        trackCount = 3,
        durationMinutes = 90,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = validTracks
      )
    }
  }

  it should "reject null session ID" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = null,
        userId = validUserId,
        trackCount = 3,
        durationMinutes = 90,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = validTracks
      )
    }
  }

  it should "reject empty session ID" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = "",
        userId = validUserId,
        trackCount = 3,
        durationMinutes = 90,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = validTracks
      )
    }
  }

  it should "reject null user ID" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = validSessionId,
        userId = null,
        trackCount = 3,
        durationMinutes = 90,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = validTracks
      )
    }
  }

  it should "reject negative track count" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = validSessionId,
        userId = validUserId,
        trackCount = -1,
        durationMinutes = 90,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = validTracks
      )
    }
  }

  it should "reject empty track list" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = validSessionId,
        userId = validUserId,
        trackCount = 0,
        durationMinutes = 90,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = List.empty
      )
    }
  }

  it should "reject inconsistent track count" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = validSessionId,
        userId = validUserId,
        trackCount = 10, // inconsistent with actual tracks
        durationMinutes = 90,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = validTracks // only 3 tracks
      )
    }
  }

  it should "reject negative duration" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = validSessionId,
        userId = validUserId,
        trackCount = 3,
        durationMinutes = -1,
        startTime = validStartTime,
        endTime = validEndTime,
        tracks = validTracks
      )
    }
  }

  it should "reject end time before start time" in {
    an[IllegalArgumentException] should be thrownBy {
      RankedSession(
        rank = 1,
        sessionId = validSessionId,
        userId = validUserId,
        trackCount = 3,
        durationMinutes = 90,
        startTime = validEndTime, // swapped
        endTime = validStartTime, // swapped
        tracks = validTracks
      )
    }
  }

  // Edge Cases
  it should "handle sessions with single track" in {
    val singleTrack = List(Track("track1", "Track One", "artist1", "Artist One"))
    
    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 1,
      durationMinutes = 3,
      startTime = validStartTime,
      endTime = validStartTime.plusSeconds(180),
      tracks = singleTrack
    )

    session.trackCount shouldBe 1
    session.tracks.size shouldBe 1
    session.uniqueTrackCount shouldBe 1
  }

  it should "handle sessions with very large track count" in {
    val manyTracks = (1 to 10000).map(i => 
      Track(s"track$i", s"Track $i", s"artist$i", s"Artist $i")
    ).toList

    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 10000,
      durationMinutes = 30000,
      startTime = validStartTime,
      endTime = validStartTime.plusSeconds(1800000),
      tracks = manyTracks
    )

    session.trackCount shouldBe 10000
    session.tracks.size shouldBe 10000
  }

  it should "handle sessions with duplicate tracks" in {
    val duplicateTracks = List(
      Track("track1", "Track One", "artist1", "Artist One"),
      Track("track1", "Track One", "artist1", "Artist One"),
      Track("track1", "Track One", "artist1", "Artist One")
    )

    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 3,
      durationMinutes = 90,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = duplicateTracks
    )

    session.trackCount shouldBe 3
    session.uniqueTrackCount shouldBe 1
  }

  it should "handle sessions spanning multiple days" in {
    val multiDayStart = Instant.parse("2023-01-01T23:00:00Z")
    val multiDayEnd = Instant.parse("2023-01-03T02:00:00Z")

    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 3,
      durationMinutes = 1620, // 27 hours
      startTime = multiDayStart,
      endTime = multiDayEnd,
      tracks = validTracks
    )

    session.durationMinutes shouldBe 1620
  }

  it should "handle tracks with special characters in metadata" in {
    val specialTracks = List(
      Track("track1", "Track♫One", "artist1", "Artîst Øne"),
      Track("track2", "Track\tTwo", "artist2", "Artist/Two"),
      Track("track3", "Track'Three", "artist3", "Artist & Three")
    )

    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 3,
      durationMinutes = 90,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = specialTracks
    )

    session.tracks.head.trackName should include("♫")
    session.tracks.head.artistName should include("î")
  }

  it should "handle tracks with Unicode names" in {
    val unicodeTracks = List(
      Track("track1", "曲一", "artist1", "アーティスト"),
      Track("track2", "Песня два", "artist2", "Художник"),
      Track("track3", "أغنية ثلاثة", "artist3", "فنان")
    )

    val session = RankedSession(
      rank = 1,
      sessionId = validSessionId,
      userId = validUserId,
      trackCount = 3,
      durationMinutes = 90,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = unicodeTracks
    )

    session.tracks.size shouldBe 3
    session.tracks.head.trackName shouldBe "曲一"
  }

  it should "provide comparison for ranking" in {
    val tracks10 = (1 to 10).map(i => 
      Track(s"track$i", s"Track $i", s"artist$i", s"Artist $i")
    ).toList
    
    val tracks5 = (1 to 5).map(i => 
      Track(s"track$i", s"Track $i", s"artist$i", s"Artist $i")
    ).toList
    
    val session1 = RankedSession(
      rank = 1,
      sessionId = "session1",
      userId = validUserId,
      trackCount = 10,
      durationMinutes = 90,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = tracks10
    )

    val session2 = RankedSession(
      rank = 2,
      sessionId = "session2",
      userId = validUserId,
      trackCount = 5,
      durationMinutes = 45,
      startTime = validStartTime,
      endTime = validEndTime,
      tracks = tracks5
    )

    session1.rank should be < session2.rank
    session1.trackCount should be > session2.trackCount
  }
}

