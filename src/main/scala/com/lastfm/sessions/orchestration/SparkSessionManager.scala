package com.lastfm.sessions.orchestration

import org.apache.spark.sql.SparkSession
import scala.util.Try

/**
 * Production Spark session management with enterprise configuration.
 * 
 * Provides centralized Spark session creation and lifecycle management
 * optimized for Last.fm session analysis workloads:
 * - Environment-aware resource allocation
 * - Production-optimized performance settings
 * - Memory management for large dataset processing (19M+ records)
 * - Proper cleanup and resource management
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object SparkSessionManager {

  /**
   * Creates production-optimized Spark session for Last.fm pipeline processing.
   * 
   * Configuration optimized for:
   * - Large dataset processing (19M+ records)
   * - Distributed operations without driver memory collection
   * - Environment-aware partitioning and resource allocation
   * - Production deployment with monitoring and error handling
   * 
   * @return SparkSession configured for production Last.fm processing
   */
  def createProductionSession(): SparkSession = {
    val cores = Runtime.getRuntime.availableProcessors()
    val optimalPartitions = cores * 3 // I/O-intensive operations
    val heapSizeGB = (Runtime.getRuntime.maxMemory() / 1024 / 1024 / 1024).toInt
    
    SparkSession.builder()
      .appName("LastFM-SessionAnalysis-Production-Pipeline")
      .master(s"local[$cores]") // Use all available cores
      .config("spark.sql.shuffle.partitions", optimalPartitions.toString)
      .config("spark.sql.session.timeZone", "UTC") // Consistent timezone handling
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // Performance
      .config("spark.sql.adaptive.enabled", "true") // Adaptive query execution
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true") // Smart coalescing
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") // Optimal partition size
      .config("spark.driver.maxResultSize", "4g") // Accommodate large results safely
      .config("spark.driver.memory", s"${Math.min(heapSizeGB - 1, 6)}g") // Reserve memory
      .config("spark.executor.memory", "2g") // Local executor memory
      .config("spark.local.dir", System.getProperty("java.io.tmpdir")) // Temp storage
      .getOrCreate()
  }

  /**
   * Creates test-optimized Spark session for unit testing.
   * 
   * Lightweight configuration for fast test execution:
   * - Minimal resource allocation for test environments
   * - Reduced parallelism for test stability
   * - Simplified configuration for CI/CD compatibility
   */
  def createTestSession(): SparkSession = {
    SparkSession.builder()
      .appName("LastFM-SessionAnalysis-Test")
      .master("local[2]") // Limited cores for testing
      .config("spark.sql.shuffle.partitions", "4") // Minimal partitions
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.ui.enabled", "false") // Disable UI for tests
      .config("spark.driver.memory", "1g") // Minimal memory for tests
      .getOrCreate()
  }

  /**
   * Properly cleans up Spark session resources.
   * 
   * Ensures graceful shutdown of Spark context and cleanup of:
   * - Temporary files and directories
   * - Thread pools and execution contexts
   * - Network connections and cluster resources
   * - Memory allocation and garbage collection
   */
  def cleanupSession(spark: SparkSession): Unit = {
    Try {
      if (spark != null && !spark.sparkContext.isStopped) {
        spark.sparkContext.setLogLevel("ERROR") // Reduce shutdown noise
        spark.stop()
      }
    }
  }
}