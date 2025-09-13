package com.lastfm.sessions.application

import com.lastfm.sessions.domain.DataRepositoryPort
import com.lastfm.sessions.infrastructure.SparkDataRepository
import org.apache.spark.sql.SparkSession

/**
 * Factory for creating data cleaning service components.
 * 
 * Provides dependency injection and configuration management for the data
 * cleaning system. Follows factory pattern to encapsulate object creation
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
object DataCleaningServiceFactory {
  
  /**
   * Creates production-ready data cleaning service.
   * 
   * Configures Spark for optimal data cleaning performance and creates
   * the complete service stack with proper optimization.
   * 
   * @param spark Spark session for distributed processing
   * @return Configured DataCleaningService ready for production use
   */
  def createProductionService(implicit spark: SparkSession): DataCleaningService = {
    // Configure Spark for data cleaning optimization
    optimizeSparkForDataCleaning(spark)
    
    // Create service with optimized Spark session
    new DataCleaningService()(spark)
  }
  
  /**
   * Optimizes Spark configuration for data cleaning workloads.
   * 
   * Applies performance optimizations specific to data cleaning:
   * - Adaptive query execution for varying data characteristics
   * - Optimal partition sizing for data validation and cleaning
   * - Memory management for large dataset processing
   * - I/O optimization for reading/writing Silver layer Parquet files
   * 
   * @param spark Spark session to optimize
   */
  private def optimizeSparkForDataCleaning(spark: SparkSession): Unit = {
    val cores = Runtime.getRuntime.availableProcessors()
    val optimalShufflePartitions = Math.max(16, cores * 2)
    
    // Adaptive query execution for varying data quality operations
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
    
    // Optimal partitioning for data cleaning operations
    spark.conf.set("spark.sql.shuffle.partitions", optimalShufflePartitions.toString)
    
    // I/O optimization for Parquet reading/writing
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.parquet.recordLevelFilter.enabled", "true")
    
    // Memory and performance optimization
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    spark.conf.set("spark.sql.optimizer.joinReorderEnabled", "true")
    
    // Data cleaning specific optimizations
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    spark.conf.set("spark.sql.broadcastTimeout", "36000")
    
    // String processing optimizations for data cleaning
    spark.conf.set("spark.sql.optimizer.inSetConversionThreshold", "10")
    spark.conf.set("spark.sql.codegen.wholeStage", "true")
    
    // Partition pruning and predicate pushdown for filtering operations
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  }
}
