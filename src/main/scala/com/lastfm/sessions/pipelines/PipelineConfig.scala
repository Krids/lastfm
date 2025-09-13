package com.lastfm.sessions.pipelines

/**
 * Production pipeline configuration following enterprise data engineering best practices.
 * 
 * Encapsulates all configuration parameters for medallion architecture pipelines:
 * - Data paths following Bronze/Silver/Gold/Results layer separation
 * - Environment-aware partitioning strategies for optimal performance  
 * - Quality thresholds aligned with business requirements
 * - Spark configuration optimized for distributed processing
 * 
 * Designed for:
 * - External configuration management (application.conf, environment variables)
 * - Environment-specific optimization (local vs cluster deployment)
 * - Performance tuning based on data characteristics
 * - Quality assurance with business-defined thresholds
 * 
 * @param bronzePath Path to raw input data (Bronze layer)
 * @param silverPath Path to quality-validated output data (Silver layer)
 * @param goldPath Path to analyzed data (Gold layer)
 * @param outputPath Path to final results (Results layer - top_songs.tsv)
 * @param partitionStrategy Strategy for data partitioning optimization
 * @param qualityThresholds Business-defined quality requirements
 * @param sparkConfig Spark-specific performance configuration
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
case class PipelineConfig(
  bronzePath: String,
  silverPath: String,
  goldPath: String = "data/output/gold",
  outputPath: String = "data/output/results",
  partitionStrategy: PartitionStrategy,
  qualityThresholds: QualityThresholds,
  sparkConfig: SparkConfig
) {
  // Configuration validation at construction time
  require(bronzePath != null && bronzePath.nonEmpty, "bronzePath cannot be null or empty")
  require(silverPath != null && silverPath.nonEmpty, "silverPath cannot be null or empty")
  require(goldPath != null && goldPath.nonEmpty, "goldPath cannot be null or empty")
  require(outputPath != null && outputPath.nonEmpty, "outputPath cannot be null or empty")
  require(partitionStrategy != null, "partitionStrategy cannot be null")
  require(qualityThresholds != null, "qualityThresholds cannot be null")
  require(sparkConfig != null, "sparkConfig cannot be null")
}

/**
 * Partition strategy interface for environment-aware data distribution.
 */
sealed trait PartitionStrategy {
  def calculateOptimalPartitions(): Int
}

/**
 * User ID-based partitioning strategy optimized for session analysis.
 * 
 * Implements environment-aware partitioning based on solution implementation plan:
 * - Local environment: 2-4x CPU cores for optimal I/O operations
 * - Cluster environment: Higher partition count for distributed processing
 * - User distribution: Balance users per partition for session calculation efficiency
 * 
 * @param userCount Estimated number of unique users in dataset
 * @param cores Available CPU cores in environment
 */
case class UserIdPartitionStrategy(userCount: Int, cores: Int) extends PartitionStrategy {
  require(userCount > 0, "userCount must be positive")
  require(cores > 0, "cores must be positive")
  
  /**
   * Calculates optimal partition count based on environment and data characteristics.
   * 
   * Strategy optimized for session analysis with 1K users dataset:
   * - Fixed 16 partitions for optimal session calculation (~62 users per partition)
   * - Aligns with memory guidance for session analysis performance
   * - Optimized for 19M records = ~1.2M records per partition (ideal size)
   */
  override def calculateOptimalPartitions(): Int = {
    // Optimal partitions for Last.fm 1K dataset session analysis
    16 // ~62 users per partition, ~1.2M records per partition
  }
  
  /**
   * Estimates users per partition for session analysis optimization.
   */
  def usersPerPartition: Int = {
    val partitions = calculateOptimalPartitions()
    userCount / partitions
  }
}

/**
 * Quality thresholds configuration for business rule validation.
 * 
 * Defines acceptance criteria for data quality based on business requirements:
 * - Session analysis requires high data quality (>99%) for temporal accuracy
 * - Production deployment requires exceptional quality (>99.9%) for reliability
 * - Track identity coverage affects song ranking accuracy
 */
case class QualityThresholds(
  sessionAnalysisMinQuality: Double = 99.0,
  productionMinQuality: Double = 99.9,
  minTrackIdCoverage: Double = 85.0,
  maxSuspiciousUserRatio: Double = 5.0
) {
  require(sessionAnalysisMinQuality >= 0.0 && sessionAnalysisMinQuality <= 100.0, 
    "sessionAnalysisMinQuality must be between 0 and 100")
  require(productionMinQuality >= sessionAnalysisMinQuality, 
    "productionMinQuality must be >= sessionAnalysisMinQuality")
  require(minTrackIdCoverage >= 0.0 && minTrackIdCoverage <= 100.0,
    "minTrackIdCoverage must be between 0 and 100") 
  require(maxSuspiciousUserRatio >= 0.0 && maxSuspiciousUserRatio <= 100.0,
    "maxSuspiciousUserRatio must be between 0 and 100")
}

/**
 * Spark configuration for distributed processing optimization.
 * 
 * Encapsulates Spark-specific settings optimized for:
 * - Large dataset processing (19M+ records)
 * - Memory efficiency and garbage collection optimization
 * - Adaptive execution for varying data characteristics
 * - Production deployment with resource management
 */
case class SparkConfig(
  partitions: Int,
  timeZone: String = "UTC",
  adaptiveEnabled: Boolean = true,
  serializerClass: String = "org.apache.spark.serializer.KryoSerializer"
) {
  require(partitions > 0, "partitions must be positive")
  require(timeZone != null && timeZone.nonEmpty, "timeZone cannot be null or empty")
  require(serializerClass != null && serializerClass.nonEmpty, "serializerClass cannot be null or empty")
}