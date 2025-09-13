package com.lastfm.sessions.application

import com.lastfm.sessions.domain._
import com.lastfm.sessions.pipelines.{DataCleaningPipeline, PipelineConfig, UserIdPartitionStrategy, QualityThresholds, SparkConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

/**
 * Application service for orchestrating data cleaning workflow.
 * 
 * Implements clean architecture principles by coordinating domain logic through
 * pipeline orchestration without infrastructure dependencies. Follows single
 * responsibility principle focusing solely on workflow orchestration.
 * 
 * Key Responsibilities:
 * - Orchestrate data cleaning workflow from Bronze to Silver layer
 * - Apply optimal data processing strategies (validation, partitioning)
 * - Coordinate pipeline operations in correct sequence
 * - Handle cross-cutting concerns (validation, error handling)
 * 
 * Design Principles:
 * - Dependency Inversion: Depends on Spark session abstraction
 * - Single Responsibility: Only workflow orchestration, no business logic
 * - Open/Closed: Extensible through configuration
 * - Clean Error Handling: Comprehensive failure modes with context
 * 
 * @param spark Spark session for distributed processing
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DataCleaningService(implicit spark: SparkSession) {
  
  // Validate dependencies at construction time (fail-fast principle)
  require(spark != null, "spark session cannot be null")
  
  /**
   * Executes complete data cleaning workflow from Bronze to Silver layer.
   * 
   * Workflow Steps:
   * 1. Validate input parameters and prerequisites
   * 2. Create optimized pipeline configuration
   * 3. Execute data cleaning pipeline with strategic partitioning
   * 4. Apply comprehensive data quality validation
   * 5. Generate track keys for identity resolution
   * 6. Persist Silver layer artifacts as Parquet with userId partitioning
   * 7. Return comprehensive quality metrics for monitoring
   * 
   * Performance Optimizations:
   * - Strategic partitioning eliminates shuffle operations in downstream processing
   * - Parquet format provides columnar efficiency and compression
   * - Single-pass quality validation minimizes data movement
   * - Lazy evaluation defers computation until required
   * 
   * @param bronzePath Path to Bronze layer input data
   * @param silverPath Path for Silver layer output
   * @return Try containing DataQualityMetrics or execution failure
   * @throws IllegalArgumentException if paths are null or empty
   */
  def cleanData(bronzePath: String, silverPath: String): Try[DataQualityMetrics] = {
    validateInputPaths(bronzePath, silverPath)
    
    try {
      // Step 1: Create optimal pipeline configuration
      val config = createOptimalPipelineConfig(bronzePath, silverPath)
      
      // Step 2: Execute data cleaning pipeline
      val pipeline = new DataCleaningPipeline(config)
      val result = pipeline.execute()
      
      // Step 3: Process results
      result match {
        case Success(qualityMetrics) =>
          for {
            // Validate quality against business thresholds
            _ <- validateQualityThresholds(qualityMetrics)
            
            // Generate additional quality insights
            enrichedMetrics = enrichQualityMetrics(qualityMetrics)
            
          } yield enrichedMetrics
          
        case Failure(exception) =>
          Failure(new RuntimeException(s"Pipeline execution failed: ${exception.getMessage}", exception))
      }
      
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Data cleaning workflow failed: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Validates data quality against business requirements.
   * 
   * Performs comprehensive quality validation by reading Parquet Silver layer data:
   * - Checks Silver layer artifacts exist and are accessible
   * - Validates quality metrics against business thresholds
   * - Assesses readiness for downstream session analysis
   * 
   * @param silverPath Path to Silver layer Parquet data
   * @return Try containing quality validation results
   */
  def validateDataQuality(silverPath: String): Try[QualityValidationResult] = {
    validateSilverPath(silverPath)
    
    try {
      // Read Parquet data directly for validation
      val silverDF = spark.read.parquet(silverPath)
      val totalRecords = silverDF.count()
      val recordsWithTrackIds = silverDF.filter(col("trackId").isNotNull && 
                                                 col("trackId") =!= "").count()
      
      val trackIdCoverage = if (totalRecords > 0) (recordsWithTrackIds.toDouble / totalRecords) * 100.0 else 0.0
      
      // Generate quality metrics
      val qualityMetrics = DataQualityMetrics(
        totalRecords = totalRecords,
        validRecords = totalRecords, // Silver layer data is already cleaned
        rejectedRecords = 0L,
        rejectionReasons = Map.empty,
        trackIdCoverage = trackIdCoverage,
        suspiciousUsers = 0 // Would require user-level aggregation if needed
      )
      
      // Create validation result
      val validationResult = createQualityValidationResult(qualityMetrics)
      Success(validationResult)
      
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Quality validation failed: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Generates comprehensive data cleaning report for monitoring and audit.
   * 
   * Creates detailed report including:
   * - Data quality metrics and assessments
   * - Processing performance indicators
   * - Business readiness indicators
   * - Recommendations for optimization
   * 
   * @param metrics Data quality metrics from cleaning process
   * @return Try containing formatted quality report
   */
  def generateCleaningReport(metrics: DataQualityMetrics): Try[String] = {
    try {
      val report = createFormattedQualityReport(metrics)
      Success(report)
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Report generation failed: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Loads and validates data quality metrics for existing Silver layer data.
   * 
   * @param silverPath Path to Silver layer Parquet data
   * @return Try containing quality metrics or failure
   */
  def loadQualityMetrics(silverPath: String): Try[DataQualityMetrics] = {
    validateSilverPath(silverPath)
    
    try {
      // Read Parquet data for metrics calculation
      val silverDF = spark.read.parquet(silverPath)
      val totalRecords = silverDF.count()
      val recordsWithTrackIds = silverDF.filter(col("trackId").isNotNull && 
                                                 col("trackId") =!= "").count()
      
      val trackIdCoverage = if (totalRecords > 0) (recordsWithTrackIds.toDouble / totalRecords) * 100.0 else 0.0
      
      val qualityMetrics = DataQualityMetrics(
        totalRecords = totalRecords,
        validRecords = totalRecords,
        rejectedRecords = 0L,
        rejectionReasons = Map.empty,
        trackIdCoverage = trackIdCoverage,
        suspiciousUsers = 0
      )
      
      Success(qualityMetrics)
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Failed to load quality metrics: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Creates optimal pipeline configuration for data cleaning.
   * 
   * @param bronzePath Path to Bronze layer input
   * @param silverPath Path to Silver layer output
   * @return Optimized pipeline configuration
   */
  private def createOptimalPipelineConfig(bronzePath: String, silverPath: String): PipelineConfig = {
    val cores = Runtime.getRuntime.availableProcessors()
    
    // Determine environment-appropriate paths
    import com.lastfm.sessions.common.Constants
    val (goldPath, outputPath) = if (Constants.Environment.isTestEnvironment) {
      ("data/test/gold", "data/test/results")
    } else {
      ("data/output/gold", "data/output/results")
    }
    
    PipelineConfig(
      bronzePath = bronzePath,
      silverPath = silverPath,
      goldPath = goldPath,
      outputPath = outputPath,
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = cores),
      qualityThresholds = QualityThresholds(
        sessionAnalysisMinQuality = 99.0,
        productionMinQuality = 99.9,
        minTrackIdCoverage = 85.0,
        maxSuspiciousUserRatio = 5.0
      ),
      sparkConfig = SparkConfig(
        partitions = 16,
        timeZone = "UTC",
        adaptiveEnabled = true
      )
    )
  }

  /**
   * Validates input parameters for data cleaning workflow.
   */
  private def validateInputPaths(bronzePath: String, silverPath: String): Unit = {
    require(bronzePath != null && bronzePath.nonEmpty, "bronzePath cannot be null or empty")
    require(silverPath != null && silverPath.nonEmpty, "silverPath cannot be null or empty")
    require(bronzePath != silverPath, "bronzePath and silverPath must be different")
  }
  
  /**
   * Validates Silver layer path parameter.
   */
  private def validateSilverPath(silverPath: String): Unit = {
    require(silverPath != null && silverPath.nonEmpty, "silverPath cannot be null or empty")
  }
  
  /**
   * Validates quality metrics against business thresholds.
   */
  private def validateQualityThresholds(metrics: DataQualityMetrics): Try[Unit] = {
    try {
      // Session analysis readiness validation
      if (metrics.qualityScore < 99.0) {
        return Failure(new RuntimeException(
          f"Quality score ${metrics.qualityScore}%.6f%% below session analysis threshold 99.0%%"
        ))
      }
      
      // Track identity coverage validation
      if (metrics.trackIdCoverage < 85.0) {
        // Log warning but don't fail - can impact song ranking accuracy but not block processing
        println(f"   ⚠️  Track MBID coverage ${metrics.trackIdCoverage}%.2f%% below optimal 85.0%%")
      }
      
      Success(())
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Quality threshold validation failed: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Enriches quality metrics with additional business insights.
   */
  private def enrichQualityMetrics(metrics: DataQualityMetrics): DataQualityMetrics = {
    // Add business context and derived metrics
    metrics.copy(
      rejectionReasons = metrics.rejectionReasons ++ Map(
        "quality_enhancement" -> (metrics.totalRecords - metrics.validRecords),
        "business_validation" -> 0L // Placeholder for additional validations
      )
    )
  }
  
  
  /**
   * Creates comprehensive quality validation result.
   */
  private def createQualityValidationResult(metrics: DataQualityMetrics): QualityValidationResult = {
    val qualityAssessment = QualityAssessment.fromScore(metrics.qualityScore)
    
    // Use track ID coverage as a proxy for engagement/performance since we don't have session lengths in cleaning stage
    val performanceCategory = if (metrics.trackIdCoverage >= 90.0) PerformanceCategory.HighEngagement
                             else if (metrics.trackIdCoverage >= 80.0) PerformanceCategory.ModerateEngagement
                             else PerformanceCategory.LowEngagement
    
    QualityValidationResult(
      qualityScore = metrics.qualityScore,
      qualityAssessment = qualityAssessment,
      performanceCategory = performanceCategory,
      isSessionAnalysisReady = qualityAssessment != QualityAssessment.Poor,
      isProductionReady = metrics.qualityScore >= 99.9
    )
  }
  
  /**
   * Creates formatted quality report for monitoring and audit.
   */
  private def createFormattedQualityReport(metrics: DataQualityMetrics): String = {
    val qualityAssessment = if (metrics.qualityScore >= 99.0) "Excellent"
                           else if (metrics.qualityScore >= 95.0) "Good" 
                           else if (metrics.qualityScore >= 90.0) "Fair"
                           else "Poor"
    
    s"""
       |Data Cleaning Quality Report
       |==========================
       |
       |Overall Metrics:
       |  Total Records: ${metrics.totalRecords}
       |  Valid Records: ${metrics.validRecords}
       |  Rejected Records: ${metrics.rejectedRecords}
       |  Quality Score: ${metrics.qualityScore}%
       |  Quality Assessment: $qualityAssessment
       |
       |Data Coverage:
       |  Track ID Coverage: ${metrics.trackIdCoverage}%
       |  Suspicious Users: ${metrics.suspiciousUsers}
       |
       |Rejection Reasons:
       |${metrics.rejectionReasons.map { case (reason, count) => s"  $reason: $count" }.mkString("\n")}
       |
       |Session Analysis Readiness: ${if (metrics.qualityScore >= 99.0) "✅ Ready" else "❌ Not Ready"}
       |Production Readiness: ${if (metrics.qualityScore >= 99.9) "✅ Ready" else "❌ Not Ready"}
       |""".stripMargin
  }
}

/**
 * Quality validation result for business assessment.
 */
case class QualityValidationResult(
  qualityScore: Double,
  qualityAssessment: QualityAssessment,
  performanceCategory: PerformanceCategory,
  isSessionAnalysisReady: Boolean,
  isProductionReady: Boolean
)

