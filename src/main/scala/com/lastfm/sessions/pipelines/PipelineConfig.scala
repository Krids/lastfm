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
  // Critical safety validation at construction time
  validatePathSafety()
  validateConfiguration()
  
  /**
   * CRITICAL SAFETY: Validates path isolation to prevent production data contamination.
   * 
   * This method implements fail-fast validation to catch configuration errors that could
   * lead to test artifacts contaminating production data directories.
   * 
   * Environment Detection Rules:
   * 1. Test Environment: All output paths must use "data/test/"
   * 2. Production Environment: Output paths must use "data/output/"  
   * 3. Cross-contamination: Prevent test paths in production and vice versa
   * 
   * @throws IllegalArgumentException if path configuration violates safety rules
   */
  private def validatePathSafety(): Unit = {
    val isTestEnvironment = detectTestEnvironment()
    val outputPaths = List(
      ("silver", silverPath),
      ("gold", goldPath), 
      ("results", outputPath)
    )
    
    if (isTestEnvironment) {
      // TEST ENVIRONMENT: Enforce test isolation
      outputPaths.foreach { case (layerName, path) =>
        require(path.startsWith("data/test"), 
          s"🚨 CRITICAL SAFETY VIOLATION: Test environment detected but $layerName path '$path' " +
          s"does not use isolated test directory. MUST use 'data/test/' prefix to prevent " +
          s"production data contamination. Current path: $path")
        
        require(!path.contains("data/output"), 
          s"🚨 CRITICAL SAFETY VIOLATION: Test environment cannot use production path: $path. " +
          s"This would contaminate production data! Use data/test/$layerName instead.")
      }
      
      println(s"✅ Test environment safety validated - all paths isolated to data/test/")
      
    } else {
      // PRODUCTION ENVIRONMENT: Ensure no test contamination
      outputPaths.foreach { case (layerName, path) =>
        require(!path.contains("test") && !path.contains("tmp"), 
          s"🚨 CRITICAL SAFETY VIOLATION: Production environment cannot use test/temp paths: $path. " +
          s"This indicates test contamination in production configuration!")
        
        require(path.startsWith("data/output") || path.startsWith("/"), 
          s"Production $layerName path must use 'data/output/' or absolute path, got: $path")
      }
      
      println(s"✅ Production environment safety validated - paths isolated from test artifacts")
    }
  }
  
  /**
   * Detects if current execution is in test environment based on multiple indicators.
   * 
   * Detection heuristics:
   * 1. Environment variable checks (ENV=test)
   * 2. JVM system properties (test frameworks set these)  
   * 3. Path analysis (presence of data/test paths)
   * 4. Stack trace analysis (test framework classes)
   * 
   * @return true if test environment detected, false for production
   */
  private def detectTestEnvironment(): Boolean = {
    // Check environment variables
    val envTest = sys.env.get("ENV").exists(_.toLowerCase.contains("test")) ||
                  sys.props.get("environment").exists(_.toLowerCase.contains("test"))
    
    // Check if any paths suggest test environment
    val pathTest = List(silverPath, goldPath, outputPath).exists(_.contains("data/test"))
    
    // Check for test framework in stack trace (ScalaTest, JUnit, etc.)
    val stackTest = Thread.currentThread().getStackTrace
      .exists(frame => 
        frame.getClassName.contains("scalatest") || 
        frame.getClassName.contains("junit") ||
        frame.getClassName.contains("Spec") ||
        frame.getClassName.contains("Test")
      )
    
    // Check JVM properties set by test frameworks
    val jvmTest = sys.props.get("sbt.testing").isDefined ||
                  sys.props.get("java.class.path").exists(_.contains("scalatest"))
    
    val isTest = envTest || pathTest || stackTest || jvmTest
    
    if (isTest) {
      println(s"🧪 Test environment detected - enforcing safety isolation")
    } else {
      println(s"🏭 Production environment detected - enforcing production safety")  
    }
    
    isTest
  }
  
  /**
   * Standard configuration validation (original logic preserved).
   */
  private def validateConfiguration(): Unit = {
    require(bronzePath != null && bronzePath.nonEmpty, "bronzePath cannot be null or empty")
    require(silverPath != null && silverPath.nonEmpty, "silverPath cannot be null or empty")
    require(goldPath != null && goldPath.nonEmpty, "goldPath cannot be null or empty")
    require(outputPath != null && outputPath.nonEmpty, "outputPath cannot be null or empty")
    require(partitionStrategy != null, "partitionStrategy cannot be null")
    require(qualityThresholds != null, "qualityThresholds cannot be null")
    require(sparkConfig != null, "sparkConfig cannot be null")
  }
  
  /**
   * Utility method to validate path at runtime during pipeline execution.
   * 
   * Can be called by pipelines before critical write operations to ensure
   * path safety hasn't been compromised during execution.
   * 
   * @param operationDescription Description of the operation being performed
   * @throws IllegalStateException if unsafe path usage detected
   */
  def validateSafeExecution(operationDescription: String): Unit = {
    try {
      validatePathSafety()
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalStateException(
          s"🚨 CRITICAL: Pipeline safety violation during '$operationDescription'. " +
          s"${e.getMessage} Execution halted to prevent data contamination.",
          e
        )
    }
  }
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