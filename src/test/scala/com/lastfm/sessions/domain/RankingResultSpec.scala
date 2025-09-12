package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Test specification for RankingResult domain model.
 * 
 * Tests final ranking results aggregation and validation.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class RankingResultSpec extends AnyFlatSpec with Matchers {

  behavior of "RankingResult"

  // Test fixtures
  private val tracks10 = (1 to 10).map(i => 
    Track(s"t$i", s"Track$i", s"a$i", s"Artist$i")
  ).toList
  
  private val tracks8 = (1 to 8).map(i => 
    Track(s"t$i", s"Track$i", s"a$i", s"Artist$i")
  ).toList
  
  private val testSessions = List(
    RankedSession(
      rank = 1,
      sessionId = "session1",
      userId = "user1",
      trackCount = 10,
      durationMinutes = 120,
      startTime = Instant.parse("2023-01-01T10:00:00Z"),
      endTime = Instant.parse("2023-01-01T12:00:00Z"),
      tracks = tracks10
    ),
    RankedSession(
      rank = 2,
      sessionId = "session2",
      userId = "user2",
      trackCount = 8,
      durationMinutes = 90,
      startTime = Instant.parse("2023-01-01T14:00:00Z"),
      endTime = Instant.parse("2023-01-01T15:30:00Z"),
      tracks = tracks8
    )
  )

  private val testTracks = List(
    TrackPopularity(1, "Track1", "Artist1", 523, 45, 30),
    TrackPopularity(2, "Track2", "Artist2", 456, 40, 28)
  )

  it should "aggregate ranking results correctly" in {
    val result = RankingResult(
      topSessions = testSessions,
      topTracks = testTracks,
      totalSessionsAnalyzed = 1000,
      totalTracksAnalyzed = 50000,
      processingTimeMillis = 2500,
      qualityScore = 98.5
    )

    result.topSessions shouldBe testSessions
    result.topTracks shouldBe testTracks
    result.totalSessionsAnalyzed shouldBe 1000
    result.totalTracksAnalyzed shouldBe 50000
    result.processingTimeMillis shouldBe 2500
    result.qualityScore shouldBe 98.5
  }

  it should "provide processing metrics" in {
    val result = RankingResult(
      topSessions = testSessions,
      topTracks = testTracks,
      totalSessionsAnalyzed = 1000,
      totalTracksAnalyzed = 50000,
      processingTimeMillis = 2500,
      qualityScore = 98.5
    )

    result.processingTimeSeconds shouldBe 2.5
    result.tracksPerSecond shouldBe 20000.0
  }

  it should "include quality validation results" in {
    val result = RankingResult(
      topSessions = testSessions,
      topTracks = testTracks,
      totalSessionsAnalyzed = 1000,
      totalTracksAnalyzed = 50000,
      processingTimeMillis = 2500,
      qualityScore = 99.5  // Above session analysis threshold
    )

    result.qualityScore shouldBe 99.5
    result.isHighQuality shouldBe true
  }

  it should "generate audit trail" in {
    val result = RankingResult(
      topSessions = testSessions,
      topTracks = testTracks,
      totalSessionsAnalyzed = 1000,
      totalTracksAnalyzed = 50000,
      processingTimeMillis = 2500,
      qualityScore = 98.5
    )

    val audit = result.auditSummary
    audit should include("Top Sessions: 2")
    audit should include("Top Tracks: 2")
    audit should include("Total Sessions Analyzed: 1000")
    audit should include("Total Tracks Analyzed: 50000")
    audit should include("Processing Time: 2.5 seconds")
    audit should include("Quality Score: 98.5%")
  }

  // Validation Tests
  it should "reject null session collection" in {
    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = null,
        topTracks = testTracks,
        totalSessionsAnalyzed = 1000,
        totalTracksAnalyzed = 50000,
        processingTimeMillis = 2500,
        qualityScore = 98.5
      )
    }
  }

  it should "reject null track collection" in {
    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = testSessions,
        topTracks = null,
        totalSessionsAnalyzed = 1000,
        totalTracksAnalyzed = 50000,
        processingTimeMillis = 2500,
        qualityScore = 98.5
      )
    }
  }

  it should "reject negative processing time" in {
    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = testSessions,
        topTracks = testTracks,
        totalSessionsAnalyzed = 1000,
        totalTracksAnalyzed = 50000,
        processingTimeMillis = -1,
        qualityScore = 98.5
      )
    }
  }

  it should "reject invalid quality score below 0" in {
    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = testSessions,
        topTracks = testTracks,
        totalSessionsAnalyzed = 1000,
        totalTracksAnalyzed = 50000,
        processingTimeMillis = 2500,
        qualityScore = -1.0
      )
    }
  }

  it should "reject invalid quality score above 100" in {
    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = testSessions,
        topTracks = testTracks,
        totalSessionsAnalyzed = 1000,
        totalTracksAnalyzed = 50000,
        processingTimeMillis = 2500,
        qualityScore = 101.0
      )
    }
  }

  it should "reject negative total sessions" in {
    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = testSessions,
        topTracks = testTracks,
        totalSessionsAnalyzed = -1,
        totalTracksAnalyzed = 50000,
        processingTimeMillis = 2500,
        qualityScore = 98.5
      )
    }
  }

  it should "reject negative total tracks" in {
    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = testSessions,
        topTracks = testTracks,
        totalSessionsAnalyzed = 1000,
        totalTracksAnalyzed = -1,
        processingTimeMillis = 2500,
        qualityScore = 98.5
      )
    }
  }

  it should "handle empty collections" in {
    val result = RankingResult(
      topSessions = List.empty,
      topTracks = List.empty,
      totalSessionsAnalyzed = 0,
      totalTracksAnalyzed = 0,
      processingTimeMillis = 100,
      qualityScore = 100.0
    )

    result.topSessions shouldBe empty
    result.topTracks shouldBe empty
  }

  it should "validate collection size constraints" in {
    val manySessions = (1 to 100).map { i =>
      val tracks = (1 to 10).map { j =>
        Track(s"t${i}_$j", s"Track${i}_$j", s"a${i}_$j", s"Artist${i}_$j")
      }.toList
      RankedSession(
        rank = i,
        sessionId = s"session$i",
        userId = s"user$i",
        trackCount = 10,
        durationMinutes = 120,
        startTime = Instant.now(),
        endTime = Instant.now().plusSeconds(7200),
        tracks = tracks
      )
    }.toList

    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = manySessions, // more than 50
        topTracks = testTracks,
        totalSessionsAnalyzed = 1000,
        totalTracksAnalyzed = 50000,
        processingTimeMillis = 2500,
        qualityScore = 98.5
      )
    }
  }

  it should "validate track collection size" in {
    val manyTracks = (1 to 20).map { i =>
      TrackPopularity(i, s"Track$i", s"Artist$i", 100, 50, 30)
    }.toList

    an[IllegalArgumentException] should be thrownBy {
      RankingResult(
        topSessions = testSessions,
        topTracks = manyTracks, // more than 10
        totalSessionsAnalyzed = 1000,
        totalTracksAnalyzed = 50000,
        processingTimeMillis = 2500,
        qualityScore = 98.5
      )
    }
  }

  it should "calculate statistics correctly" in {
    val result = RankingResult(
      topSessions = testSessions,
      topTracks = testTracks,
      totalSessionsAnalyzed = 1000,
      totalTracksAnalyzed = 50000,
      processingTimeMillis = 2500,
      qualityScore = 98.5
    )

    result.averageSessionsPerTrack shouldBe 500.0 // 1000 / 2
    result.compressionRatio shouldBe 5000.0 // 50000 / 10 (assuming top 10)
  }
}

