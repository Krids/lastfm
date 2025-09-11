package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

/**
 * Test specification for SessionAnalysis domain model.
 * 
 * Tests comprehensive session aggregation and analysis functionality:
 * - Session collection management and validation
 * - Statistical analysis and metrics calculation
 * - Session ranking by multiple criteria with consistent tie handling
 * - Top-N session extraction with boundary conditions
 * 
 * Follows TDD approach with clear test organization and expressive naming
 * that describes expected behavior for business stakeholders.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionAnalysisSpec extends AnyFlatSpec with Matchers {

  // Test data fixtures representing realistic session patterns
  private val baseTime = Instant.parse("2023-01-15T14:30:00Z")
  
  private def createSession(userId: String, trackCount: Int, startMinuteOffset: Int = 0): UserSession = {
    val tracks = (0 until trackCount).map { i =>
      ListenEvent(
        userId = userId,
        timestamp = baseTime.plusSeconds((startMinuteOffset + i * 3) * 60L), // 3-minute gaps
        artistId = Some(s"artist-$i"),
        artistName = s"Artist $i",
        trackId = Some(s"track-$i"),
        trackName = s"Track $i",
        trackKey = s"track-$i"
      )
    }.toList
    
    UserSession(userId, tracks)
  }
  
  // Sample session collection for testing
  private val sampleSessions = List(
    createSession("user_001", trackCount = 50), // Long session
    createSession("user_002", trackCount = 25), // Medium session  
    createSession("user_003", trackCount = 10), // Short session
    createSession("user_001", trackCount = 30), // Second session for user_001
    createSession("user_004", trackCount = 75)  // Very long session
  )

  /**
   * Happy Path Tests - Core functionality with valid inputs
   */
  "SessionAnalysis creation" should "create analysis with valid session collection" in {
    // Arrange
    val sessions = sampleSessions
    
    // Act & Assert - This should fail initially (Red phase)
    val analysis = SessionAnalysis(sessions)
    
    analysis.sessions should have size 5
    analysis.totalSessions should be(5)
    analysis.uniqueUsers should be(4)
  }
  
  it should "calculate total tracks across all sessions" in {
    // Arrange
    val sessions = sampleSessions
    
    // Act
    val analysis = SessionAnalysis(sessions)
    
    // Assert - Sum: 50 + 25 + 10 + 30 + 75 = 190
    analysis.totalTracks should be(190)
  }
  
  it should "handle empty session collection" in {
    // Arrange
    val sessions = List.empty[UserSession]
    
    // Act
    val analysis = SessionAnalysis(sessions)
    
    // Assert
    analysis.sessions should be(empty)
    analysis.totalSessions should be(0)
    analysis.uniqueUsers should be(0)
    analysis.totalTracks should be(0)
  }
  
  it should "provide accurate session statistics" in {
    // Arrange
    val sessions = sampleSessions
    
    // Act  
    val analysis = SessionAnalysis(sessions)
    
    // Assert
    analysis.averageSessionLength should be(38.0) // 190 / 5
    analysis.longestSession.trackCount should be(75)
    analysis.shortestSession.trackCount should be(10)
  }

  /**
   * Session Ranking Tests - Ordering and selection logic
   */
  "SessionAnalysis ranking" should "rank sessions by track count descending" in {
    // Arrange
    val sessions = sampleSessions
    val analysis = SessionAnalysis(sessions)
    
    // Act
    val rankedSessions = analysis.sessionsByTrackCount
    
    // Assert - Should be ordered: 75, 50, 30, 25, 10
    rankedSessions.map(_.trackCount) should be(List(75, 50, 30, 25, 10))
  }
  
  it should "extract top N sessions correctly" in {
    // Arrange
    val sessions = sampleSessions
    val analysis = SessionAnalysis(sessions)
    
    // Act
    val top3Sessions = analysis.topSessions(3)
    
    // Assert
    top3Sessions should have size 3
    top3Sessions.map(_.trackCount) should be(List(75, 50, 30))
  }
  
  it should "handle request for more sessions than available" in {
    // Arrange
    val sessions = sampleSessions  
    val analysis = SessionAnalysis(sessions)
    
    // Act
    val top10Sessions = analysis.topSessions(10)
    
    // Assert - Should return all available sessions
    top10Sessions should have size 5
    top10Sessions.map(_.trackCount) should be(List(75, 50, 30, 25, 10))
  }
  
  it should "handle request for zero sessions" in {
    // Arrange
    val sessions = sampleSessions
    val analysis = SessionAnalysis(sessions)
    
    // Act
    val zeroSessions = analysis.topSessions(0)
    
    // Assert
    zeroSessions should be(empty)
  }

  /**
   * Edge Cases - Complex scenarios and boundary conditions
   */
  "SessionAnalysis edge cases" should "handle ties in session length consistently" in {
    // Arrange - Create sessions with identical track counts
    val sessions = List(
      createSession("user_001", trackCount = 20, startMinuteOffset = 0),
      createSession("user_002", trackCount = 20, startMinuteOffset = 100), // Later start time
      createSession("user_003", trackCount = 20, startMinuteOffset = 50)   // Middle start time
    )
    
    // Act
    val analysis = SessionAnalysis(sessions)
    val topSessions = analysis.topSessions(3)
    
    // Assert - Should use consistent tiebreaker (e.g., start time)
    topSessions should have size 3
    topSessions.forall(_.trackCount == 20) should be(true)
    // Verify stable ordering by checking user IDs remain consistent across runs
    val userIds = topSessions.map(_.userId)
    userIds should be(userIds.sorted) // Should be consistently ordered
  }
  
  it should "handle sessions with identical metadata" in {
    // Arrange - Sessions that are completely identical except timestamp
    val baseSession = createSession("user_001", trackCount = 15)
    val identicalSessions = List(baseSession, baseSession, baseSession)
    
    // Act
    val analysis = SessionAnalysis(identicalSessions)
    
    // Assert
    analysis.totalSessions should be(3)
    analysis.uniqueUsers should be(1)
    analysis.topSessions(2) should have size 2
  }
  
  it should "handle large session collections efficiently" in {
    // Arrange - Generate 1000 sessions with random track counts
    val largeSessions = (1 to 1000).map { i =>
      val trackCount = (i % 100) + 1 // 1-100 tracks per session
      createSession(s"user_${i % 100}", trackCount) // 100 unique users
    }.toList
    
    // Act
    val analysis = SessionAnalysis(largeSessions)
    
    // Assert
    analysis.totalSessions should be(1000)
    analysis.uniqueUsers should be(100)
    // Performance check - should complete without timeout
    analysis.topSessions(50) should have size 50
  }

  /**
   * Validation Tests - Input validation and error cases  
   */
  "SessionAnalysis validation" should "reject null session list" in {
    // Act & Assert
    intercept[IllegalArgumentException] {
      SessionAnalysis(null)
    }.getMessage should include("sessions cannot be null")
  }

  /**
   * Statistical Analysis Tests - Mathematical calculations
   */
  "SessionAnalysis statistics" should "calculate session distribution metrics" in {
    // Arrange
    val sessions = sampleSessions
    val analysis = SessionAnalysis(sessions)
    
    // Act & Assert
    analysis.sessionDistribution should contain key 50 // trackCount -> count
    analysis.sessionDistribution should contain key 25
    analysis.sessionDistribution should contain key 10
    analysis.sessionDistribution should contain key 30
    analysis.sessionDistribution should contain key 75
    
    // Each track count appears once
    analysis.sessionDistribution.values should contain only 1
  }
  
  it should "identify user activity patterns" in {
    // Arrange
    val sessions = sampleSessions
    val analysis = SessionAnalysis(sessions)
    
    // Act
    val userStats = analysis.userStatistics
    
    // Assert
    userStats should contain key "user_001"
    userStats("user_001").sessionCount should be(2) // Two sessions
    userStats("user_001").totalTracks should be(80) // 50 + 30
    
    userStats should contain key "user_004"
    userStats("user_004").sessionCount should be(1)
    userStats("user_004").totalTracks should be(75)
  }

  /**
   * Business Logic Tests - Domain-specific calculations
   */
  "SessionAnalysis business logic" should "provide session quality indicators" in {
    // Arrange
    val sessions = sampleSessions
    val analysis = SessionAnalysis(sessions)
    
    // Act & Assert
    // Sessions with >40 tracks considered "high quality"
    analysis.highQualitySessionCount should be(2) // 50 and 75 track sessions
    analysis.qualityScore should be > 0.0
    analysis.qualityScore should be <= 100.0
  }
  
  it should "maintain immutability after creation" in {
    // Arrange
    val originalSessions = sampleSessions
    val analysis = SessionAnalysis(originalSessions)
    
    // Act - Get the session list
    val analysisSessions = analysis.sessions
    
    // Assert - Should be immutable
    analysisSessions should be(originalSessions)
    analysisSessions shouldBe a[List[_]]
  }
}