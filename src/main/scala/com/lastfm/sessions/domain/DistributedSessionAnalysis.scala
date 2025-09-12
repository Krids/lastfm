package com.lastfm.sessions.domain

/**
 * Domain model for distributed session analysis following clean architecture principles.
 * 
 * Encapsulates session analysis business logic without infrastructure dependencies.
 * Designed for memory efficiency by working with metrics rather than raw data collections.
 * 
 * Key Features:
 * - Memory-efficient design (works with aggregated metrics, not raw data)
 * - Pure domain logic with no infrastructure coupling
 * - Immutable value object with comprehensive validation
 * - Clear business rules for quality and performance assessment
 * - Fail-fast validation at construction time
 * 
 * @param metrics Aggregated session metrics from distributed processing
 * @throws IllegalArgumentException if metrics parameter is null
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class DistributedSessionAnalysis(metrics: SessionMetrics) {
  
  // Fail-fast validation (Clean Code principle)
  require(metrics != null, "metrics cannot be null")
  
  /**
   * Provides quality assessment based on data quality score.
   * Business logic for interpreting quality metrics.
   */
  def qualityAssessment: QualityAssessment = {
    QualityAssessment.fromScore(metrics.qualityScore)
  }
  
  /**
   * Provides performance category based on user engagement patterns.
   * Business logic for categorizing session behavior.
   */
  def performanceCategory: PerformanceCategory = {
    PerformanceCategory.fromSessionLength(metrics.averageSessionLength)
  }
  
  /**
   * Determines if the analysis indicates a successful listening ecosystem.
   * Business rule combining quality and engagement metrics.
   */
  def isSuccessfulEcosystem: Boolean = {
    qualityAssessment == QualityAssessment.Excellent && 
    performanceCategory == PerformanceCategory.HighEngagement
  }
  
  /**
   * Calculates user activity score as a composite metric.
   * Combines session frequency and engagement for overall activity assessment.
   */
  def userActivityScore: Double = {
    if (metrics.uniqueUsers == 0) 0.0
    else {
      val sessionsPerUser = metrics.totalSessions.toDouble / metrics.uniqueUsers
      val engagementMultiplier = performanceCategory match {
        case PerformanceCategory.HighEngagement => 1.5
        case PerformanceCategory.ModerateEngagement => 1.0
        case PerformanceCategory.LowEngagement => 0.5
      }
      sessionsPerUser * engagementMultiplier
    }
  }
}

/**
 * Quality assessment enumeration for session analysis.
 * Represents different levels of data quality based on business thresholds.
 */
sealed trait QualityAssessment

object QualityAssessment {
  case object Poor extends QualityAssessment
  case object Fair extends QualityAssessment
  case object Good extends QualityAssessment
  case object Excellent extends QualityAssessment
  
  /**
   * Converts quality score to assessment category.
   * Business rules for quality thresholds.
   */
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
 * Represents different levels of user engagement with the platform.
 */
sealed trait PerformanceCategory

object PerformanceCategory {
  case object LowEngagement extends PerformanceCategory
  case object ModerateEngagement extends PerformanceCategory
  case object HighEngagement extends PerformanceCategory
  
  /**
   * Converts average session length to performance category.
   * Business rules for engagement thresholds.
   */
  def fromSessionLength(averageLength: Double): PerformanceCategory = {
    averageLength match {
      case l if l >= 50.0 => HighEngagement
      case l if l >= 20.0 => ModerateEngagement
      case _ => LowEngagement
    }
  }
}
