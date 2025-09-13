package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * TDD-First specification for distributed SessionAnalysis domain model.
 * 
 * Tests session analysis functionality without loading data into memory.
 * Focuses on domain logic and business rules for session analysis.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DistributedSessionAnalysisSpec extends AnyFlatSpec with Matchers {

  "SessionAnalysis" should "calculate metrics without loading data into memory" in {
    // Given: Session metrics (representing distributed calculation results)
    val sessionMetrics = SessionMetrics(
      totalSessions = 50000L,
      uniqueUsers = 1000L,
      totalTracks = 2500000L,
      averageSessionLength = 50.0,
      qualityScore = 99.5
    )
    
    // When: Analysis is performed
    val analysis = DistributedSessionAnalysis(sessionMetrics)
    
    // Then: Metrics are available without memory issues
    analysis.metrics.totalSessions shouldBe 50000L
    analysis.metrics.uniqueUsers shouldBe 1000L
    analysis.metrics.totalTracks shouldBe 2500000L
    analysis.metrics.averageSessionLength shouldBe 50.0
    analysis.metrics.qualityScore shouldBe 99.5
  }
  
  it should "provide quality assessment based on quality score" in {
    // Given: Analysis with high quality score
    val highQualityMetrics = SessionMetrics(
      totalSessions = 50000L,
      uniqueUsers = 1000L,
      totalTracks = 2500000L,
      averageSessionLength = 50.0,
      qualityScore = 99.5
    )
    val analysis = DistributedSessionAnalysis(highQualityMetrics)
    
    // When: Quality assessment is requested
    val qualityAssessment = analysis.qualityAssessment
    
    // Then: Should indicate excellent quality
    qualityAssessment shouldBe QualityAssessment.Excellent
  }
  
  it should "provide performance category based on session length" in {
    // Given: Analysis with high average session length
    val longSessionMetrics = SessionMetrics(
      totalSessions = 50000L,
      uniqueUsers = 1000L,
      totalTracks = 2500000L,
      averageSessionLength = 75.0, // High session length
      qualityScore = 99.5
    )
    val analysis = DistributedSessionAnalysis(longSessionMetrics)
    
    // When: Performance category is requested
    val performanceCategory = analysis.performanceCategory
    
    // Then: Should indicate high engagement
    performanceCategory shouldBe PerformanceCategory.HighEngagement
  }
  
  it should "handle empty dataset metrics" in {
    // Given: Empty dataset metrics
    val emptyMetrics = SessionMetrics(
      totalSessions = 0L,
      uniqueUsers = 0L,
      totalTracks = 0L,
      averageSessionLength = 0.0,
      qualityScore = 0.0
    )
    
    // When: Analysis is performed
    val analysis = DistributedSessionAnalysis(emptyMetrics)
    
    // Then: Should handle empty case gracefully
    analysis.metrics.totalSessions shouldBe 0L
    analysis.qualityAssessment shouldBe QualityAssessment.Poor
    analysis.performanceCategory shouldBe PerformanceCategory.LowEngagement
  }
  
  it should "reject null metrics" in {
    // When: Creating analysis with null metrics
    // Then: Should throw IllegalArgumentException
    intercept[IllegalArgumentException] {
      DistributedSessionAnalysis(null)
    }
  }
}

/**
 * Quality assessment enumeration for session analysis.
 */
sealed trait QualityAssessment

object QualityAssessment {
  case object Poor extends QualityAssessment
  case object Fair extends QualityAssessment
  case object Good extends QualityAssessment
  case object Excellent extends QualityAssessment
  
  def fromScore(score: Double): QualityAssessment = {
    score match {
      case s if s >= 95.0 => Excellent
      case s if s >= 85.0 => Good
      case s if s >= 70.0 => Fair
      case _ => Poor
    }
  }
}

/**
 * Performance category enumeration based on user engagement.
 */
sealed trait PerformanceCategory

object PerformanceCategory {
  case object LowEngagement extends PerformanceCategory
  case object ModerateEngagement extends PerformanceCategory
  case object HighEngagement extends PerformanceCategory
  
  def fromSessionLength(averageLength: Double): PerformanceCategory = {
    averageLength match {
      case l if l >= 50.0 => HighEngagement
      case l if l >= 20.0 => ModerateEngagement
      case _ => LowEngagement
    }
  }
}
