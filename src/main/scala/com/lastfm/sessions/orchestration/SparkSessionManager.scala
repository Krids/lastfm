package com.lastfm.sessions.orchestration

import org.apache.spark.sql.SparkSession
import com.lastfm.sessions.common.{Constants, ConfigurableConstants}
import com.lastfm.sessions.common.traits.{SparkConfigurable, MetricsCalculator}
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
object SparkSessionManager extends SparkConfigurable with MetricsCalculator {

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
    val optimalPartitions = ConfigurableConstants.Partitioning.DEFAULT_PARTITIONS.value
    val heapSizeGB = (Runtime.getRuntime.maxMemory() / 1024 / 1024 / 1024).toInt
    
    val builder = SparkSession.builder()
      .appName("LastFM-SessionAnalysis-Production-Pipeline")
    
    // Apply production configuration using trait
    configureForProduction(builder)
      .config("spark.driver.memory", s"${Math.min(heapSizeGB - 2, 16)}g")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.maxResultSize", "8g")
      .config("spark.local.dir", System.getProperty("java.io.tmpdir"))
      .config("spark.driver.extraJavaOptions", getOptimalJVMOptions(heapSizeGB))
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
    val builder = SparkSession.builder()
      .appName("LastFM-SessionAnalysis-Test")
    
    // Apply test configuration using trait
    configureForTesting(builder).getOrCreate()
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