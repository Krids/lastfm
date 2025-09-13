package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test specification for DataQualityMetrics domain model.
 * 
 * Tests align with real Last.fm dataset characteristics discovered in exploratory analysis:
 * - 19,150,868 total records
 * - 8 empty track names (0.00004%)
 * - 2 exact duplicates (0.00001%)  
 * - 2,168,588 missing track MBIDs (11.3%)
 * - 13 users with >100k plays (potential bot detection)
 * 
 * Implements Stage 1: Critical Field Validation from data cleaning strategy.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DataQualityMetricsSpec extends AnyFlatSpec with Matchers {

  /**
   * Tests for successful DataQualityMetrics creation with real data patterns.
   */
  "DataQualityMetrics creation" should "handle Last.fm dataset characteristics accurately" in {
    // Arrange - Based on actual notebook analysis results
    val metrics = DataQualityMetrics(
      totalRecords = 19150868L,
      validRecords = 19150860L,    // 8 empty track names dropped
      rejectedRecords = 8L,
      rejectionReasons = Map(
        "empty_track_name" -> 8L,
        "exact_duplicates" -> 0L   // Will be handled separately
      ),
      trackIdCoverage = 88.7,      // 11.3% missing MBIDs (acceptable)
      suspiciousUsers = 13L        // Users with >100k plays
    )
    
    // Assert - Real data quality thresholds
    metrics.totalRecords should be(19150868L)
    metrics.validRecords should be(19150860L)
    metrics.rejectedRecords should be(8L)
    metrics.trackIdCoverage should be(88.7 +- 0.1)
    metrics.suspiciousUsers should be(13L)
  }
  
  it should "calculate quality score correctly" in {
    // Arrange
    val totalRecords = 1000L
    val validRecords = 950L
    val expectedScore = 95.0
    
    // Act
    val metrics = DataQualityMetrics(
      totalRecords = totalRecords,
      validRecords = validRecords,
      rejectedRecords = 50L,
      rejectionReasons = Map("validation_failed" -> 50L),
      trackIdCoverage = 85.0,
      suspiciousUsers = 2L
    )
    
    // Assert - Only test score calculation
    metrics.qualityScore should be(95.0)
  }

  it should "calculate quality thresholds based on session analysis needs" in {
    // Arrange - Session analysis requires: userId + timestamp + track identity
    val sessionReadyMetrics = DataQualityMetrics(
      totalRecords = 1000L, 
      validRecords = 990L, 
      rejectedRecords = 10L, 
      rejectionReasons = Map("minor_issues" -> 10L), 
      trackIdCoverage = 88.0, 
      suspiciousUsers = 0L
    )
    
    // Act & Assert
    sessionReadyMetrics.qualityScore should be(99.0)
    sessionReadyMetrics.isSessionAnalysisReady should be(true) // >99% valid
    sessionReadyMetrics.hasAcceptableTrackCoverage should be(true) // >85% MBID coverage
  }
  
  it should "flag suspicious user patterns for investigation" in {
    // Arrange
    val metricsWithBots = DataQualityMetrics(
      totalRecords = 1000L, 
      validRecords = 1000L, 
      rejectedRecords = 0L, 
      rejectionReasons = Map(), 
      trackIdCoverage = 90.0, 
      suspiciousUsers = 50L
    )
    
    // Act & Assert
    metricsWithBots.hasSuspiciousUsers should be(true)
    metricsWithBots.suspiciousUserRatio should be(5.0) // 5% suspicious users
  }

  /**
   * Tests for validation rules - each test validates one specific constraint.
   */
  "DataQualityMetrics validation" should "reject negative total records" in {
    // Act & Assert
    an [IllegalArgumentException] should be thrownBy {
      DataQualityMetrics(
        totalRecords = -1L,
        validRecords = 0L,
        rejectedRecords = 0L,
        rejectionReasons = Map(),
        trackIdCoverage = 90.0,
        suspiciousUsers = 0L
      )
    }
  }
  
  it should "reject negative valid records" in {
    // Act & Assert
    an [IllegalArgumentException] should be thrownBy {
      DataQualityMetrics(
        totalRecords = 100L,
        validRecords = -1L,
        rejectedRecords = 101L,
        rejectionReasons = Map(),
        trackIdCoverage = 90.0,
        suspiciousUsers = 0L
      )
    }
  }
  
  it should "reject inconsistent record counts" in {
    // Act & Assert - valid + rejected must equal total
    an [IllegalArgumentException] should be thrownBy {
      DataQualityMetrics(
        totalRecords = 100L,
        validRecords = 60L,
        rejectedRecords = 50L,  // 60 + 50 = 110 != 100
        rejectionReasons = Map(),
        trackIdCoverage = 90.0,
        suspiciousUsers = 0L
      )
    }
  }
  
  it should "reject invalid track ID coverage" in {
    // Act & Assert - coverage must be 0-100%
    an [IllegalArgumentException] should be thrownBy {
      DataQualityMetrics(
        totalRecords = 100L,
        validRecords = 90L,
        rejectedRecords = 10L,
        rejectionReasons = Map(),
        trackIdCoverage = 150.0,  // Invalid percentage
        suspiciousUsers = 0L
      )
    }
  }

  /**
   * Tests for real-world quality assessment methods.
   */
  "DataQualityMetrics quality assessment" should "identify production-ready quality" in {
    // Arrange - Based on actual Last.fm dataset quality
    val excellentQuality = DataQualityMetrics(
      totalRecords = 19150868L,
      validRecords = 19150860L,
      rejectedRecords = 8L,
      rejectionReasons = Map("empty_track_name" -> 8L),
      trackIdCoverage = 88.7,
      suspiciousUsers = 13L
    )
    
    // Act & Assert
    excellentQuality.qualityScore should be > 99.9
    excellentQuality.isProductionReady should be(true)
    excellentQuality.isExcellent should be(true)
  }
  
  it should "identify acceptable quality for analysis" in {
    // Arrange - Good but not excellent quality
    val goodQuality = DataQualityMetrics(
      totalRecords = 1000L,
      validRecords = 990L,
      rejectedRecords = 10L,
      rejectionReasons = Map("minor_issues" -> 10L),
      trackIdCoverage = 87.0,
      suspiciousUsers = 1L
    )
    
    // Act & Assert
    goodQuality.qualityScore should be(99.0)
    goodQuality.isAcceptable should be(true)
    goodQuality.isSessionAnalysisReady should be(true)
  }
  
  it should "identify problematic quality requiring investigation" in {
    // Arrange - Quality too low for reliable analysis
    val poorQuality = DataQualityMetrics(
      totalRecords = 1000L,
      validRecords = 850L,
      rejectedRecords = 150L,
      rejectionReasons = Map("major_issues" -> 150L),
      trackIdCoverage = 60.0,  // Low MBID coverage
      suspiciousUsers = 100L   // High bot activity
    )
    
    // Act & Assert
    poorQuality.qualityScore should be(85.0)
    poorQuality.isAcceptable should be(false)
    poorQuality.isSessionAnalysisReady should be(false)
    poorQuality.needsInvestigation should be(true)
  }

  /**
   * Tests for business logic quality metrics specific to session analysis.
   */
  "DataQualityMetrics session analysis readiness" should "require high data quality" in {
    // Arrange - Marginal quality
    val marginalQuality = DataQualityMetrics(
      totalRecords = 1000L,
      validRecords = 980L,  // 98% - marginal for session analysis
      rejectedRecords = 20L,
      rejectionReasons = Map("validation_issues" -> 20L),
      trackIdCoverage = 85.0,
      suspiciousUsers = 0L
    )
    
    // Act & Assert - Session analysis needs >99% quality
    marginalQuality.qualityScore should be(98.0)
    marginalQuality.isSessionAnalysisReady should be(false) // Below 99% threshold
  }
  
  it should "require acceptable track identity coverage" in {
    // Arrange - High quality but poor track coverage
    val poorTrackCoverage = DataQualityMetrics(
      totalRecords = 1000L,
      validRecords = 995L,
      rejectedRecords = 5L,
      rejectionReasons = Map("minor_issues" -> 5L),
      trackIdCoverage = 70.0,  // Below 85% threshold
      suspiciousUsers = 0L
    )
    
    // Act & Assert
    poorTrackCoverage.qualityScore should be(99.5)
    poorTrackCoverage.hasAcceptableTrackCoverage should be(false)
    poorTrackCoverage.isSessionAnalysisReady should be(false) // Poor track identity
  }
}