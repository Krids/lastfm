package com.lastfm.sessions.application

import com.lastfm.sessions.domain._
import com.lastfm.sessions.application.QualityValidationResult
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

/**
 * Application service for orchestrating distributed session analysis workflow.
 * 
 * Implements clean architecture principles by coordinating domain logic through
 * repository abstractions without infrastructure dependencies. Follows single
 * responsibility principle focusing solely on workflow orchestration.
 * 
 * Key Responsibilities:
 * - Orchestrate session analysis workflow from Silver to Gold layer
 * - Apply optimal data processing strategies (partitioning, caching)
 * - Coordinate repository operations in correct sequence
 * - Handle cross-cutting concerns (validation, error handling)
 * - Provide clean API for pipeline integration
 * 
 * Design Principles:
 * - Dependency Inversion: Depends on repository abstraction, not implementation
 * - Single Responsibility: Only workflow orchestration, no business logic
 * - Open/Closed: Extensible through repository implementations
 * - Interface Segregation: Clean, focused public API
 * - Clean Error Handling: Comprehensive failure modes with context
 * 
 * @param repository Repository implementation for data operations
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionAnalysisService(repository: DistributedSessionAnalysisRepository) {
  
  // Validate dependencies at construction time (fail-fast principle)
  require(repository != null, "repository cannot be null")
  
  /**
   * Executes complete session analysis workflow from Silver to Gold layer.
   * 
   * Workflow Steps:
   * 1. Load listening events from Silver layer as distributed stream
   * 2. Apply optimal partitioning by userId for session analysis
   * 3. Cache stream for multiple operations (performance optimization)
   * 4. Calculate session metrics using distributed aggregations
   * 5. Create comprehensive session analysis with business insights
   * 6. Persist analysis results to Gold layer
   * 7. Return analysis for further processing or monitoring
   * 
   * Performance Optimizations:
   * - User-based partitioning eliminates shuffle operations during session calculation
   * - Stream caching enables efficient reuse across multiple operations
   * - Single-pass distributed aggregations minimize data movement
   * - Lazy evaluation defers computation until required
   * 
   * @param silverPath Path to cleaned Silver layer data
   * @param goldPath Base path for Gold layer output
   * @return Try containing SessionAnalysis results or failure
   * @throws IllegalArgumentException if paths are null or empty
   */
  def analyzeUserSessions(silverPath: String, goldPath: String): Try[DistributedSessionAnalysis] = {
    // Validate input parameters
    validateInputParameters(silverPath, goldPath)
    
    try {
      for {
        // Step 1: Load events as distributed stream
        eventsStream <- repository.loadEventsStream(silverPath)
        
        // Step 2: Apply optimal processing strategies
        optimizedStream = applyOptimalProcessingStrategies(eventsStream)
        
        // Step 3: Calculate session metrics using distributed operations
        metrics <- repository.calculateSessionMetrics(optimizedStream)
        
        // Step 4: Create comprehensive analysis with business insights
        analysis = createComprehensiveAnalysis(metrics)
        
        // Step 5: Persist results to Gold layer
        _ <- repository.persistAnalysis(analysis, goldPath)
        
      } yield analysis
      
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Session analysis failed: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Generates top sessions ranking using distributed processing.
   * 
   * Optimized for large-scale session ranking without driver memory constraints.
   * 
   * @param silverPath Path to Silver layer data
   * @param topN Number of top sessions to return
   * @return Try containing top sessions list
   */
  def generateTopSessionsRanking(silverPath: String, topN: Int): Try[List[SessionSummary]] = {
    validateSilverPath(silverPath)
    require(topN > 0, "topN must be positive")
    
    for {
      eventsStream <- repository.loadEventsStream(silverPath)
      optimizedStream = applyOptimalProcessingStrategies(eventsStream)
      topSessions <- repository.generateTopSessions(optimizedStream, topN)
    } yield topSessions
  }
  
  /**
   * Validates session analysis quality against business thresholds.
   * 
   * @param silverPath Path to Silver layer data
   * @return Try containing quality validation results
   */
  def validateSessionAnalysisQuality(silverPath: String): Try[QualityValidationResult] = {
    validateSilverPath(silverPath)
    
    for {
      eventsStream <- repository.loadEventsStream(silverPath)
      optimizedStream = applyOptimalProcessingStrategies(eventsStream)
      metrics <- repository.calculateSessionMetrics(optimizedStream)
      analysis = createComprehensiveAnalysis(metrics)
    } yield QualityValidationResult(
      qualityScore = metrics.qualityScore,
      qualityAssessment = analysis.qualityAssessment,
      performanceCategory = analysis.performanceCategory,
      isSessionAnalysisReady = analysis.qualityAssessment != QualityAssessment.Poor,
      isProductionReady = analysis.isSuccessfulEcosystem
    )
  }
  
  /**
   * Applies optimal processing strategies for session analysis performance.
   * 
   * Strategy Implementation:
   * - Partition by userId to eliminate shuffle during session calculation
   * - Cache stream for reuse across multiple operations
   * - Apply any additional optimizations based on data characteristics
   * 
   * @param eventsStream Input event stream
   * @return Optimized event stream
   */
  private def applyOptimalProcessingStrategies(eventsStream: EventStream): EventStream = {
    eventsStream
      .partitionByUser()  // Optimize for session analysis groupBy operations
      .cache()           // Enable efficient reuse across multiple operations
  }
  
  /**
   * Creates comprehensive session analysis with business insights.
   * 
   * Enriches raw metrics with business logic and interpretations.
   * 
   * @param metrics Raw session metrics from distributed calculations
   * @return Enhanced session analysis with business context
   */
  private def createComprehensiveAnalysis(metrics: SessionMetrics): DistributedSessionAnalysis = {
    DistributedSessionAnalysis(metrics)
  }
  
  /**
   * Validates input parameters for session analysis operations.
   * 
   * @param silverPath Silver layer path
   * @param goldPath Gold layer path
   * @throws IllegalArgumentException if parameters are invalid
   */
  private def validateInputParameters(silverPath: String, goldPath: String): Unit = {
    validateSilverPath(silverPath)
    validateGoldPath(goldPath)
  }
  
  /**
   * Validates Silver layer path parameter.
   * 
   * @param silverPath Path to validate
   * @throws IllegalArgumentException if path is invalid
   */
  private def validateSilverPath(silverPath: String): Unit = {
    require(silverPath != null, "silverPath cannot be null")
    require(silverPath.nonEmpty, "silverPath cannot be empty")
    require(!silverPath.isBlank, "silverPath cannot be blank")
  }
  
  /**
   * Validates Gold layer path parameter.
   * 
   * @param goldPath Path to validate
   * @throws IllegalArgumentException if path is invalid
   */
  private def validateGoldPath(goldPath: String): Unit = {
    require(goldPath != null, "goldPath cannot be null")
    require(goldPath.nonEmpty, "goldPath cannot be empty")
    require(!goldPath.isBlank, "goldPath cannot be blank")
  }
}

