package com.lastfm.sessions.domain.services

import com.lastfm.sessions.domain._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Test specification for SessionRanker domain service.
 * 
 * Tests session ranking logic with deterministic tie-breaking.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionRankerSpec extends AnyFlatSpec with Matchers {

  behavior of "SessionRanker"

  // Test fixtures
  private def createSession(
    sessionId: String,
    userId: String,
    trackCount: Int,
    durationMinutes: Long,
    startTime: Instant = Instant.parse("2023-01-01T10:00:00Z")
  ): RankedSession = {
    val tracks = (1 to trackCount).map { i =>
      Track(s"t$i", s"Track$i", s"a$i", s"Artist$i")
    }.toList
    
    RankedSession(
      rank = 1, // Will be reassigned by ranker
      sessionId = sessionId,
      userId = userId,
      trackCount = trackCount,
      durationMinutes = durationMinutes,
      startTime = startTime,
      endTime = startTime.plusSeconds(durationMinutes * 60),
      tracks = tracks
    )
  }

  it should "rank sessions by track count descending" in {
    val sessions = List(
      createSession("s1", "user1", trackCount = 5, durationMinutes = 60),
      createSession("s2", "user2", trackCount = 10, durationMinutes = 120),
      createSession("s3", "user3", trackCount = 3, durationMinutes = 30)
    )

    val ranker = new SessionRanker()
    val ranked = ranker.rankSessions(sessions)

    ranked.map(_.trackCount) shouldBe List(10, 5, 3)
    ranked.map(_.rank) shouldBe List(1, 2, 3)
  }

  it should "use duration as tiebreaker for equal track counts" in {
    val sessions = List(
      createSession("s1", "user1", trackCount = 10, durationMinutes = 60),
      createSession("s2", "user2", trackCount = 10, durationMinutes = 120),
      createSession("s3", "user3", trackCount = 10, durationMinutes = 90)
    )

    val ranker = new SessionRanker()
    val ranked = ranker.rankSessions(sessions)

    ranked.map(_.durationMinutes) shouldBe List(120, 90, 60)
    ranked.map(_.rank) shouldBe List(1, 2, 3)
  }

  it should "use start time as secondary tiebreaker" in {
    val time1 = Instant.parse("2023-01-01T10:00:00Z")
    val time2 = Instant.parse("2023-01-01T11:00:00Z")
    val time3 = Instant.parse("2023-01-01T09:00:00Z")
    
    val sessions = List(
      createSession("s1", "user1", 10, 120, time1),
      createSession("s2", "user2", 10, 120, time2),
      createSession("s3", "user3", 10, 120, time3)
    )

    val ranker = new SessionRanker()
    val ranked = ranker.rankSessions(sessions)

    ranked.map(_.startTime) shouldBe List(time3, time1, time2) // Earliest first
    ranked.map(_.rank) shouldBe List(1, 2, 3)
  }

  it should "use session ID for final deterministic ordering" in {
    val time = Instant.parse("2023-01-01T10:00:00Z")
    
    val sessions = List(
      createSession("session_c", "user1", 10, 120, time),
      createSession("session_a", "user2", 10, 120, time),
      createSession("session_b", "user3", 10, 120, time)
    )

    val ranker = new SessionRanker()
    val ranked = ranker.rankSessions(sessions)

    ranked.map(_.sessionId) shouldBe List("session_a", "session_b", "session_c")
    ranked.map(_.rank) shouldBe List(1, 2, 3)
  }

  it should "return exactly N sessions when requested" in {
    val sessions = (1 to 100).map { i =>
      createSession(s"s$i", s"user$i", trackCount = Math.max(1, 101 - i), durationMinutes = 60)
    }.toList

    val ranker = new SessionRanker()
    val top50 = ranker.getTopSessions(sessions, 50)

    top50.size shouldBe 50
    top50.head.trackCount shouldBe 100 // Highest
    top50.last.trackCount shouldBe 51 // 50th highest
  }

  it should "handle request for more sessions than available" in {
    val sessions = List(
      createSession("s1", "user1", 10, 60),
      createSession("s2", "user2", 5, 30)
    )

    val ranker = new SessionRanker()
    val top50 = ranker.getTopSessions(sessions, 50)

    top50.size shouldBe 2
  }

  it should "handle empty session collection" in {
    val ranker = new SessionRanker()
    val ranked = ranker.rankSessions(List.empty)

    ranked shouldBe empty
  }

  it should "handle request for zero sessions" in {
    val sessions = List(
      createSession("s1", "user1", 10, 60),
      createSession("s2", "user2", 5, 30)
    )

    val ranker = new SessionRanker()
    val top0 = ranker.getTopSessions(sessions, 0)

    top0 shouldBe empty
  }

  it should "handle sessions with identical track counts consistently" in {
    val sessions = List(
      createSession("s1", "user1", 10, 60),
      createSession("s2", "user2", 10, 60),
      createSession("s3", "user3", 10, 60),
      createSession("s4", "user4", 10, 60),
      createSession("s5", "user5", 10, 60)
    )

    val ranker = new SessionRanker()
    val ranked1 = ranker.rankSessions(sessions)
    val ranked2 = ranker.rankSessions(sessions)

    ranked1.map(_.sessionId) shouldEqual ranked2.map(_.sessionId)
  }

  it should "maintain stable sorting across multiple runs" in {
    val sessions = (1 to 20).map { i =>
      createSession(s"s$i", s"user$i", trackCount = (i % 5) + 1, durationMinutes = 60)
    }.toList

    val ranker = new SessionRanker()
    val run1 = ranker.rankSessions(sessions).map(_.sessionId)
    val run2 = ranker.rankSessions(sessions).map(_.sessionId)
    val run3 = ranker.rankSessions(sessions).map(_.sessionId)

    run1 shouldEqual run2
    run2 shouldEqual run3
  }

  it should "handle very large session collections efficiently" in {
    val largeSessions = (1 to 1000).map { i =>
      createSession(s"s$i", s"user$i", trackCount = 100 - (i % 100), durationMinutes = 60)
    }.toList

    val ranker = new SessionRanker()
    val start = System.currentTimeMillis()
    val top50 = ranker.getTopSessions(largeSessions, 50)
    val duration = System.currentTimeMillis() - start

    top50.size shouldBe 50
    duration should be < 1000L // Should complete in less than 1 second
  }

  it should "handle sessions from same user with same metrics" in {
    val sessions = List(
      createSession("s1", "user1", 10, 60),
      createSession("s2", "user1", 10, 60),
      createSession("s3", "user1", 10, 60)
    )

    val ranker = new SessionRanker()
    val ranked = ranker.rankSessions(sessions)

    ranked.size shouldBe 3
    ranked.map(_.userId).distinct shouldBe List("user1")
  }

  it should "assign sequential ranks without gaps" in {
    val sessions = (1 to 10).map { i =>
      createSession(s"s$i", s"user$i", trackCount = i, durationMinutes = 60)
    }.toList

    val ranker = new SessionRanker()
    val ranked = ranker.rankSessions(sessions)

    ranked.map(_.rank) shouldBe (1 to 10).toList
  }
}