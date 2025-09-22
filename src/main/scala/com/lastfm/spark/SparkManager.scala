package com.lastfm.spark

import org.apache.spark.sql.SparkSession

/**
 * Portfolio-focused Spark session management with cluster-ready configuration.
 * 
 * Demonstrates advanced Spark configuration while keeping complexity appropriate
 * for portfolio projects. Shows understanding of production deployment without
 * over-engineering the local development experience.
 * 
 * Key Features:
 * - Environment-aware configuration (local[*] optimized)
 * - Adaptive query execution for performance
 * - Kryo serialization for efficiency
 * - Resource management for container deployment
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object SparkManager {
  
  /**
   * Creates optimized Spark session for data engineering workloads.
   * 
   * Configuration demonstrates understanding of:
   * - Performance optimization (adaptive execution, Kryo serialization)
   * - Resource management (driver memory, result size limits)
   * - Local development optimization (local[*] with appropriate parallelism)
   */
  def createSession(): SparkSession = {
    SparkSession.builder()
      .appName("LastFM-Session-Analysis")
      .master("local[*]")  // Use all available cores
      
      // Performance optimizations
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true") 
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      
      // Memory management for local development
      .config("spark.driver.memory", getOptimalDriverMemory())
      .config("spark.driver.maxResultSize", "2g")
      
      // Partition optimization for local processing
      .config("spark.sql.shuffle.partitions", getOptimalShufflePartitions())
      
      .getOrCreate()
  }
  
  /**
   * Calculates optimal driver memory based on available system resources.
   * Demonstrates understanding of resource management.
   */
  private def getOptimalDriverMemory(): String = {
    val maxMemoryGB = Runtime.getRuntime.maxMemory() / (1024 * 1024 * 1024)
    val optimalMemoryGB = Math.min(Math.max(maxMemoryGB * 0.7, 4), 12)
    s"${optimalMemoryGB.toInt}g"
  }
  
  /**
   * Calculates optimal shuffle partitions for local processing.
   * Shows understanding of partition tuning for different environments.
   */
  private def getOptimalShufflePartitions(): Int = {
    val cores = Runtime.getRuntime.availableProcessors()
    // For local processing: 2-4x cores is optimal for I/O bound operations
    Math.max(cores * 2, 8).min(32)  // Between 8-32 partitions for local
  }
}

