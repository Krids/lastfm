package com.lastfm.sessions.application

import com.lastfm.sessions.domain.DistributedSessionAnalysisRepository
import com.lastfm.sessions.infrastructure.SparkDistributedSessionAnalysisRepository
import org.apache.spark.sql.SparkSession

/**
 * Factory for creating distributed session analysis components.
 * 
 * Provides dependency injection and configuration management for the distributed
 * session analysis system. Follows factory pattern to encapsulate object creation
 * and enable easy testing and environment-specific configuration.
 * 
 * Key Responsibilities:
 * - Create properly configured repository implementations
 * - Inject dependencies for application services
 * - Provide environment-specific optimizations
 * - Enable easy mocking and testing
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object DistributedSessionAnalysisFactory {
  
  /**
   * Creates production-ready distributed session analysis service.
   * 
   * Configures Spark for optimal session analysis performance and creates
   * the complete service stack with proper dependency injection.
   * 
   * @param spark Spark session for distributed processing
   * @return Configured SessionAnalysisService ready for production use
   */
  def createProductionService(implicit spark: SparkSession): SessionAnalysisService = {
    // Configure Spark for session analysis optimization
    optimizeSparkForSessionAnalysis(spark)
    
    // Create repository with Spark implementation
    val repository = createSparkRepository(spark)
    
    // Create service with injected repository
    new SessionAnalysisService(repository)
  }
  
  /**
   * Creates test-friendly session analysis service.
   * 
   * Uses simplified configuration suitable for unit and integration testing.
   * 
   * @param spark Test Spark session
   * @return SessionAnalysisService configured for testing
   */
  def createTestService(implicit spark: SparkSession): SessionAnalysisService = {
    val repository = createSparkRepository(spark)
    new SessionAnalysisService(repository)
  }
  
  /**
   * Creates service with custom repository implementation.
   * 
   * Enables dependency injection for testing and custom implementations.
   * 
   * @param repository Custom repository implementation
   * @return SessionAnalysisService with injected repository
   */
  def createServiceWithRepository(repository: DistributedSessionAnalysisRepository): SessionAnalysisService = {
    new SessionAnalysisService(repository)
  }
  
  /**
   * Creates Spark-based repository implementation.
   * 
   * @param spark Spark session for distributed processing
   * @return SparkDistributedSessionAnalysisRepository
   */
  private def createSparkRepository(spark: SparkSession): DistributedSessionAnalysisRepository = {
    new SparkDistributedSessionAnalysisRepository()(spark)
  }
  
  /**
   * Optimizes Spark configuration for session analysis workloads.
   * 
   * Applies performance optimizations specific to session analysis:
   * - Adaptive query execution for varying data characteristics
   * - Optimal partition sizing for session calculation
   * - Memory management for large dataset processing
   * - Kryo serialization for performance
   * 
   * @param spark Spark session to optimize
   */
  private def optimizeSparkForSessionAnalysis(spark: SparkSession): Unit = {
    val cores = Runtime.getRuntime.availableProcessors()
    val optimalShufflePartitions = Math.max(16, cores * 4)
    
    // Adaptive query execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    // Optimal partitioning for session analysis
    spark.conf.set("spark.sql.shuffle.partitions", optimalShufflePartitions.toString)
    
    // Memory and performance optimization (skip serializer - already set at session creation)
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    spark.conf.set("spark.sql.optimizer.joinReorderEnabled", "true")
    
    // Session analysis specific optimizations
    // Note: timeZone already set at session creation
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false") // Scala optimization
  }
}
