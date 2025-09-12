package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * TDD-First specification for SessionMetrics domain model.
 * 
 * Tests domain logic and validation rules for session analysis metrics.
 * Follows clean code principles with clear test names and behavior verification.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionMetricsSpec extends AnyFlatSpec with Matchers {

  "SessionMetrics" should "create valid metrics with positive values" in {
    // Given: Valid metric values
    val metrics = SessionMetrics(
      totalSessions = 50000L,
      uniqueUsers = 1000L,
      totalTracks = 2500000L,
      averageSessionLength = 50.0,
      qualityScore = 99.5
    )
    
    // Then: All values should be accessible
    metrics.totalSessions shouldBe 50000L
    metrics.uniqueUsers shouldBe 1000L
    metrics.totalTracks shouldBe 2500000L
    metrics.averageSessionLength shouldBe 50.0
    metrics.qualityScore shouldBe 99.5
  }
  
  it should "reject negative total sessions" in {
    // When: Creating metrics with negative total sessions
    // Then: Should throw IllegalArgumentException
    intercept[IllegalArgumentException] {
      SessionMetrics(
        totalSessions = -1L,
        uniqueUsers = 1000L,
        totalTracks = 2500000L,
        averageSessionLength = 50.0,
        qualityScore = 99.5
      )
    }
  }
  
  it should "reject negative unique users" in {
    // When: Creating metrics with negative unique users
    // Then: Should throw IllegalArgumentException
    intercept[IllegalArgumentException] {
      SessionMetrics(
        totalSessions = 50000L,
        uniqueUsers = -1L,
        totalTracks = 2500000L,
        averageSessionLength = 50.0,
        qualityScore = 99.5
      )
    }
  }
  
  it should "reject negative total tracks" in {
    // When: Creating metrics with negative total tracks
    // Then: Should throw IllegalArgumentException
    intercept[IllegalArgumentException] {
      SessionMetrics(
        totalSessions = 50000L,
        uniqueUsers = 1000L,
        totalTracks = -1L,
        averageSessionLength = 50.0,
        qualityScore = 99.5
      )
    }
  }
  
  it should "reject negative average session length" in {
    // When: Creating metrics with negative average session length
    // Then: Should throw IllegalArgumentException
    intercept[IllegalArgumentException] {
      SessionMetrics(
        totalSessions = 50000L,
        uniqueUsers = 1000L,
        totalTracks = 2500000L,
        averageSessionLength = -1.0,
        qualityScore = 99.5
      )
    }
  }
  
  it should "reject quality score below 0" in {
    // When: Creating metrics with quality score below 0
    // Then: Should throw IllegalArgumentException
    intercept[IllegalArgumentException] {
      SessionMetrics(
        totalSessions = 50000L,
        uniqueUsers = 1000L,
        totalTracks = 2500000L,
        averageSessionLength = 50.0,
        qualityScore = -1.0
      )
    }
  }
  
  it should "reject quality score above 100" in {
    // When: Creating metrics with quality score above 100
    // Then: Should throw IllegalArgumentException
    intercept[IllegalArgumentException] {
      SessionMetrics(
        totalSessions = 50000L,
        uniqueUsers = 1000L,
        totalTracks = 2500000L,
        averageSessionLength = 50.0,
        qualityScore = 101.0
      )
    }
  }
  
  it should "accept quality score of exactly 0" in {
    // Given: Quality score of exactly 0
    val metrics = SessionMetrics(
      totalSessions = 50000L,
      uniqueUsers = 1000L,
      totalTracks = 2500000L,
      averageSessionLength = 50.0,
      qualityScore = 0.0
    )
    
    // Then: Should be valid
    metrics.qualityScore shouldBe 0.0
  }
  
  it should "accept quality score of exactly 100" in {
    // Given: Quality score of exactly 100
    val metrics = SessionMetrics(
      totalSessions = 50000L,
      uniqueUsers = 1000L,
      totalTracks = 2500000L,
      averageSessionLength = 50.0,
      qualityScore = 100.0
    )
    
    // Then: Should be valid
    metrics.qualityScore shouldBe 100.0
  }
  
  it should "handle zero values appropriately" in {
    // Given: Zero values for metrics
    val metrics = SessionMetrics(
      totalSessions = 0L,
      uniqueUsers = 0L,
      totalTracks = 0L,
      averageSessionLength = 0.0,
      qualityScore = 0.0
    )
    
    // Then: Should be valid (empty dataset case)
    metrics.totalSessions shouldBe 0L
    metrics.uniqueUsers shouldBe 0L
    metrics.totalTracks shouldBe 0L
    metrics.averageSessionLength shouldBe 0.0
    metrics.qualityScore shouldBe 0.0
  }
}
