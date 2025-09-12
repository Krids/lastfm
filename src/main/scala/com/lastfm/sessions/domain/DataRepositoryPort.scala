package com.lastfm.sessions.domain

import scala.util.Try

/**
 * Port interface for loading listening event data.
 * 
 * This interface defines the contract for data loading adapters,
 * allowing the domain layer to remain independent of specific
 * data sources (files, databases, APIs, etc.).
 * 
 * Enhanced to support comprehensive data quality assessment and
 * validation during the loading process.
 */
trait DataRepositoryPort {
  /**
   * Loads listening events from the specified path.
   * 
   * @param path Path to the data source
   * @return Try containing either a list of listening events or an error
   */
  def loadListenEvents(path: String): Try[List[ListenEvent]]

  /**
   * Loads listening events with comprehensive data quality assessment.
   * 
   * This enhanced method performs:
   * - Critical field validation (userId, timestamp)
   * - Data standardization (artist/track names, Unicode handling)
   * - Quality enhancement (deduplication, track key generation)
   * - Comprehensive quality metrics collection
   * 
   * Based on Last.fm dataset analysis requirements:
   * - Handle 8 empty track names per 19M records
   * - Process 2 exact duplicates appropriately
   * - Generate track keys with 88.7% MBID coverage
   * - Flag 13 suspicious users with >100k plays
   * 
   * @param path Path to the data source
   * @return Try containing tuple of (validated events, quality metrics) or error
   */
  def loadWithDataQuality(path: String): Try[(List[ListenEvent], DataQualityMetrics)]

  /**
   * Cleans raw data and persists as artifacts for downstream consumption.
   * 
   * Implements medallion architecture pattern:
   * - Bronze Layer (input): Raw Last.fm TSV data
   * - Silver Layer (output): Quality-validated, cleaned TSV data + JSON quality report
   * 
   * This method enables:
   * - Data Quality Context separation from Session Analysis Context
   * - Strategic caching through persistent cleaned data artifacts
   * - Performance optimization by avoiding re-cleaning on each pipeline run
   * - Complete audit trail and data lineage tracking
   * 
   * Artifacts generated:
   * - {outputPath}: Cleaned TSV file with validated listening events
   * - {outputPath-dir}/quality-report.json: Comprehensive quality metrics
   * 
   * @param inputPath Path to raw TSV data source (Bronze layer)
   * @param outputPath Path to cleaned TSV output (Silver layer)
   * @return Try containing quality metrics or error
   */
  def cleanAndPersist(inputPath: String, outputPath: String): Try[DataQualityMetrics]

  /**
   * Loads cleaned listening events from Silver layer for session analysis.
   * 
   * Optimized for session analysis processing:
   * - Loads quality-validated events from Silver layer artifacts
   * - Applies optimal partitioning strategy for session calculation
   * - Handles large datasets efficiently with distributed processing
   * - Maintains data lineage from Silver layer processing
   * 
   * @param silverPath Path to Silver layer cleaned data (TSV format)
   * @return Try containing list of validated listening events or error
   */
  def loadCleanedEvents(silverPath: String): Try[List[ListenEvent]]

  /**
   * Persists session analysis results to Gold layer for downstream consumption.
   * 
   * Implements Silver → Gold transformation with comprehensive artifacts:
   * - Gold Layer: Session analysis results with top sessions ranking
   * - Session metrics: Statistical analysis and quality indicators
   * - Data lineage: Complete audit trail from Silver to Gold processing
   * 
   * Artifacts generated:
   * - {goldPath}/sessions.json: Complete session analysis results
   * - {goldPath}/top-sessions.json: Top N longest sessions (configurable)
   * - {goldPath}/session-metrics.json: Statistical analysis and quality metrics
   * 
   * @param sessions Session analysis results to persist
   * @param goldPath Path to Gold layer output directory
   * @param topSessionCount Number of top sessions to extract (default 50)
   * @return Try containing success confirmation or error
   */
  // Note: persistSessionAnalysis method removed as it's replaced by 
  // DistributedSessionAnalysisRepository with different interface
}