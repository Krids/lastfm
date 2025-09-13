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
    val optimalPartitions = 16 // Optimal for 1K users session analysis (~62 users per partition)
    val heapSizeGB = (Runtime.getRuntime.maxMemory() / 1024 / 1024 / 1024).toInt
    
    SparkSession.builder()
      .appName("LastFM-SessionAnalysis-Production-Pipeline")
      .master(s"local[$cores]") // Use all available cores
      .config("spark.sql.shuffle.partitions", optimalPartitions.toString)
      .config("spark.sql.session.timeZone", "UTC") // Consistent timezone handling
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // Performance
      .config("spark.sql.adaptive.enabled", "true") // Adaptive query execution
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true") // Smart coalescing
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") // Larger partitions for 19M records
      .config("spark.driver.maxResultSize", "8g") // Accommodate large results for 19M records
      .config("spark.driver.memory", s"${Math.min(heapSizeGB - 2, 16)}g") // More memory for large dataset
      .config("spark.executor.memory", "4g") // Increased executor memory
      .config("spark.local.dir", System.getProperty("java.io.tmpdir")) // Temp storage
      .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "16") // Start with optimal partitions
      .config("spark.sql.adaptive.skewJoin.enabled", "true") // Handle data skew
      .config("spark.sql.files.maxPartitionBytes", "134217728") // 128MB per file partition
      .config("spark.sql.execution.arrow.pyspark.enabled", "false") // Disable for Scala optimization
      .config("spark.driver.extraJavaOptions", getOptimalJVMOptions()) // Enterprise JVM tuning
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
   * Generates optimal JVM options for enterprise-scale data processing.
   * 
   * Optimized for 96GB RAM systems processing 19M+ records:
   * - G1GC for large heap efficiency (>32GB)
   * - Low-latency garbage collection settings
   * - Memory optimization for big data workloads
   * - Performance tuning for Spark driver operations
   */
  private def getOptimalJVMOptions(): String = {
    val maxHeapGB = Runtime.getRuntime.maxMemory() / 1024 / 1024 / 1024
    
    if (maxHeapGB >= 32) {
      // High-memory enterprise configuration (32GB+)
      "-XX:+UseG1GC " +
      "-XX:G1HeapRegionSize=16m " +
      "-XX:+G1UseAdaptiveIHOP " +
      "-XX:G1MixedGCCountTarget=8 " +
      "-XX:G1HeapWastePercent=5 " +
      "-XX:+UnlockExperimentalVMOptions " +
      "-XX:+UseLargePages " +
      "-XX:+AlwaysPreTouch " +
      "-XX:+DisableExplicitGC " +
      "-Djava.awt.headless=true"
    } else if (maxHeapGB >= 8) {
      // Medium-memory configuration (8-32GB) 
      "-XX:+UseG1GC " +
      "-XX:G1HeapRegionSize=8m " +
      "-XX:+G1UseAdaptiveIHOP " +
      "-Djava.awt.headless=true"
    } else {
      // Default configuration (<8GB)
      "-XX:+UseParallelGC " +
      "-Djava.awt.headless=true"
    }
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