package com.lastfm.sessions.domain

/**
 * Comprehensive data quality metrics for data cleaning pipeline.
 * 
 * Captures quality statistics and provides business logic methods
 * to assess whether data is suitable for session analysis.
 * 
 * Based on real-world Last.fm dataset analysis:
 * - 19,150,868 total records with 99.999958% quality score
 * - 88.7% track MBID coverage (acceptable for analysis)
 * - 8 empty track names requiring cleanup
 * - 13 suspicious users with >100k plays
 * 
 * @param totalRecords Total number of input records processed
 * @param validRecords Number of records that passed all validation
 * @param rejectedRecords Number of records rejected due to quality issues
 * @param rejectionReasons Breakdown of rejection reasons with counts
 * @param trackIdCoverage Percentage of records with track MBIDs (0-100)
 * @param suspiciousUsers Number of users with anomalous play patterns
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class DataQualityMetrics(
  totalRecords: Long,
  validRecords: Long,
  rejectedRecords: Long,
  rejectionReasons: Map[String, Long],
  trackIdCoverage: Double,
  suspiciousUsers: Long
) {

  // Validation of inputs
  require(totalRecords >= 0, "Total records cannot be negative")
  require(validRecords >= 0, "Valid records cannot be negative")
  require(rejectedRecords >= 0, "Rejected records cannot be negative")
  require(validRecords + rejectedRecords == totalRecords, 
    s"Valid records ($validRecords) + rejected records ($rejectedRecords) must equal total records ($totalRecords)")
  require(trackIdCoverage >= 0.0 && trackIdCoverage <= 100.0, 
    s"Track ID coverage must be between 0 and 100, got $trackIdCoverage")
  require(suspiciousUsers >= 0, "Suspicious users cannot be negative")

  /**
   * Overall data quality score as percentage (0-100).
   * 
   * Calculated as (validRecords / totalRecords) * 100
   */
  def qualityScore: Double = {
    if (totalRecords == 0) 100.0
    else (validRecords.toDouble / totalRecords) * 100.0
  }

  /**
   * Data quality assessment for general production use.
   * 
   * @return true if quality score >= 95%
   */
  def isAcceptable: Boolean = qualityScore >= 95.0

  /**
   * Data quality assessment for session analysis specifically.
   * 
   * Session analysis requires high data quality due to:
   * - Temporal ordering dependencies (timestamp precision)
   * - User session boundaries (userId reliability) 
   * - Track counting accuracy (track identity consistency)
   * 
   * @return true if quality score >= 99% and track coverage >= 85%
   */
  def isSessionAnalysisReady: Boolean = {
    qualityScore >= 99.0 && hasAcceptableTrackCoverage
  }

  /**
   * Production readiness assessment for sensitive data processing.
   * 
   * @return true if quality score >= 99.9%
   */
  def isProductionReady: Boolean = qualityScore >= 99.9

  /**
   * Excellence threshold for data quality.
   * 
   * @return true if quality score >= 99.9%
   */
  def isExcellent: Boolean = qualityScore >= 99.9

  /**
   * Track identity coverage assessment.
   * 
   * Based on Last.fm analysis showing 88.7% MBID coverage is sufficient
   * for accurate track counting and song ranking.
   * 
   * @return true if track ID coverage >= 85%
   */
  def hasAcceptableTrackCoverage: Boolean = trackIdCoverage >= 85.0

  /**
   * Suspicious user detection assessment.
   * 
   * @return true if any users show anomalous patterns
   */
  def hasSuspiciousUsers: Boolean = suspiciousUsers > 0

  /**
   * Calculate percentage of suspicious users relative to total users.
   * 
   * Estimates total users from valid records (assuming ~1000 users in dataset).
   * Used for bot detection and data quality assessment.
   */
  def suspiciousUserRatio: Double = {
    val estimatedUsers = Math.max(1000L, validRecords / 100000) // Rough estimate
    (suspiciousUsers.toDouble / estimatedUsers) * 100.0
  }

  /**
   * Indicates whether data quality requires investigation.
   * 
   * @return true if quality is below acceptable thresholds or shows concerning patterns
   */
  def needsInvestigation: Boolean = {
    !isAcceptable || !hasAcceptableTrackCoverage || (suspiciousUserRatio > 2.0)
  }
}