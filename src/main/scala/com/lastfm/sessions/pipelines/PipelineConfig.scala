package com.lastfm.sessions.pipelines

/**
 * Production pipeline configuration following enterprise data engineering best practices.
 * 
 * Encapsulates all configuration parameters for medallion architecture pipelines:
 * - Data paths following Bronze/Silver/Gold layer separation
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
  partitionStrategy: PartitionStrategy,
  qualityThresholds: QualityThresholds,
  sparkConfig: SparkConfig
) {
  // Configuration validation at construction time
  require(bronzePath != null && bronzePath.nonEmpty, "bronzePath cannot be null or empty")
  require(silverPath != null && silverPath.nonEmpty, "silverPath cannot be null or empty")
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
   * Strategy based on memory reference: Use 2-4x CPU cores for local processing
   * to match compute resources rather than arbitrarily high partition counts.
   */
  override def calculateOptimalPartitions(): Int = {
    // Base calculation on cores (2-4x multiplier aligned with memory reference)
    val basedOnCores = cores * 2 // 2x cores for optimal local processing
    
    // Consider user distribution (target ~50-100 users per partition)
    val basedOnUsers = Math.max(userCount / 62, cores) // ~62 users per partition as per memory
    
    // Use the larger of the two for optimal performance
    Math.max(basedOnCores, basedOnUsers)
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