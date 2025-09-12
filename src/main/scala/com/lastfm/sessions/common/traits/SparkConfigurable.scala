package com.lastfm.sessions.common.traits

import org.apache.spark.sql.SparkSession
import com.lastfm.sessions.common.{Constants, ConfigurableConstants}

/**
 * Trait providing common Spark configuration patterns.
 * 
 * Encapsulates best practices for Spark session configuration,
 * environment-aware optimization, and resource management.
 * 
 * Design Principles:
 * - Environment-aware configuration (local vs cluster)
 * - Resource optimization based on available hardware
 * - Performance tuning for specific workload patterns
 * - Consistent configuration across pipeline components
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait SparkConfigurable {
  
  /**
   * Configures Spark session builder for production workloads.
   * 
   * Applies optimizations for:
   * - Large dataset processing (19M+ records)
   * - Distributed operations without driver memory collection
   * - Adaptive query execution
   * - Optimal partitioning strategies
   * 
   * @param builder SparkSession builder to configure
   * @return Configured SparkSession builder
   */
  def configureForProduction(builder: SparkSession.Builder): SparkSession.Builder = {
    val cores = Runtime.getRuntime.availableProcessors()
    val partitions = ConfigurableConstants.Partitioning.DEFAULT_PARTITIONS.value
    val cacheLevel = ConfigurableConstants.Cache.STORAGE_LEVEL.value
    
    builder
      .master(s"local[$cores]") // Use all available cores
      .config("spark.sql.shuffle.partitions", partitions.toString)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.execution.arrow.pyspark.enabled", "false")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.enableVectorizedReader", "true")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.default.parallelism", partitions.toString)
      .config("spark.sql.files.maxPartitionBytes", "134217728") // 128MB
      .config("spark.sql.broadcastTimeout", "36000") // 10 minutes
      .config("spark.network.timeout", "800s")
      .config("spark.executor.heartbeatInterval", "60s")
  }
  
  /**
   * Configures Spark session builder for testing environments.
   * 
   * Lightweight configuration optimized for:
   * - Fast test execution
   * - Minimal resource usage
   * - Test stability and isolation
   * - CI/CD compatibility
   * 
   * @param builder SparkSession builder to configure
   * @return Configured SparkSession builder for testing
   */
  def configureForTesting(builder: SparkSession.Builder): SparkSession.Builder = {
    builder
      .master("local[2]") // Limited cores for testing
      .config("spark.sql.shuffle.partitions", "4") // Minimal partitions
      .config("spark.sql.adaptive.enabled", "false") // Disable for predictable testing
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.ui.enabled", "false") // Disable UI for tests
      .config("spark.driver.memory", "1g") // Minimal memory for tests
      .config("spark.executor.memory", "512m")
      .config("spark.driver.maxResultSize", "512m")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }
  
  /**
   * Calculates optimal partition count based on data characteristics and environment.
   * 
   * Strategy:
   * - Target users per partition for session analysis efficiency
   * - Scale with available CPU cores
   * - Respect min/max partition bounds
   * - Account for memory constraints
   * 
   * @param userCount Estimated number of unique users
   * @param cores Available CPU cores
   * @return Optimal partition count
   */
  def calculateOptimalPartitions(userCount: Int, cores: Int): Int = {
    val targetUsersPerPartition = ConfigurableConstants.Partitioning.USERS_PER_PARTITION_TARGET.value
    val minPartitions = ConfigurableConstants.Partitioning.MIN_PARTITIONS.value
    val maxPartitions = ConfigurableConstants.Partitioning.MAX_PARTITIONS.value
    val partitionMultiplier = Constants.Partitioning.PARTITION_MULTIPLIER
    
    // Calculate based on data distribution
    val dataBasedPartitions = Math.max(userCount / targetUsersPerPartition, 1)
    
    // Calculate based on available compute resources
    val coreBasedPartitions = cores * partitionMultiplier
    
    // Use the larger of the two approaches, but respect bounds
    val optimalPartitions = Math.max(dataBasedPartitions, coreBasedPartitions)
    
    // Apply min/max constraints
    Math.max(minPartitions, Math.min(optimalPartitions, maxPartitions))
  }
  
  /**
   * Optimizes memory settings based on available system memory.
   * 
   * @param totalMemoryGB Total available system memory in GB
   * @return Map of memory-related Spark configurations
   */
  def getOptimalMemorySettings(totalMemoryGB: Int): Map[String, String] = {
    val driverMemoryGB = Math.min(totalMemoryGB - 2, 16) // Leave 2GB for OS, max 16GB for driver
    val executorMemoryGB = Math.min(4, totalMemoryGB / 4) // Conservative executor memory
    
    Map(
      "spark.driver.memory" -> s"${driverMemoryGB}g",
      "spark.executor.memory" -> s"${executorMemoryGB}g",
      "spark.driver.maxResultSize" -> "8g",
      "spark.executor.memoryFraction" -> "0.8",
      "spark.storage.memoryFraction" -> "0.5"
    )
  }
  
  /**
   * Gets JVM optimization flags based on available memory.
   * 
   * @param heapSizeGB Available heap size in GB
   * @return JVM optimization flags
   */
  def getOptimalJVMOptions(heapSizeGB: Int): String = {
    if (heapSizeGB >= 32) {
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
    } else if (heapSizeGB >= 8) {
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
   * Validates Spark configuration for common issues.
   * 
   * @param spark SparkSession to validate
   * @return List of validation warnings/errors
   */
  def validateSparkConfiguration(spark: SparkSession): List[String] = {
    val issues = scala.collection.mutable.ListBuffer[String]()
    val conf = spark.conf
    
    // Check partition configuration
    val shufflePartitions = conf.get("spark.sql.shuffle.partitions").toInt
    val cores = Runtime.getRuntime.availableProcessors()
    if (shufflePartitions > cores * 10) {
      issues += s"Too many shuffle partitions ($shufflePartitions) for $cores cores. Consider reducing."
    }
    
    // Check memory configuration
    val driverMemory = conf.getOption("spark.driver.memory").getOrElse("1g")
    if (driverMemory == "1g") {
      issues += "Driver memory is at default 1g. Consider increasing for large datasets."
    }
    
    // Check serialization
    val serializer = conf.getOption("spark.serializer").getOrElse("default")
    if (!serializer.contains("Kryo")) {
      issues += "Consider using KryoSerializer for better performance."
    }
    
    // Check adaptive query execution
    val adaptiveEnabled = conf.getOption("spark.sql.adaptive.enabled").exists(_.toBoolean)
    if (!adaptiveEnabled) {
      issues += "Adaptive Query Execution is disabled. Enable for better performance."
    }
    
    issues.toList
  }
  
  /**
   * Applies dataset-specific optimizations based on data characteristics.
   * 
   * @param spark SparkSession to configure
   * @param recordCount Estimated number of records
   * @param userCount Estimated number of users
   */
  def optimizeForDataset(spark: SparkSession, recordCount: Long, userCount: Int): Unit = {
    val conf = spark.conf
    
    // Optimize based on data size
    if (recordCount > 10000000) { // 10M+ records
      conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
      conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "8")
    }
    
    // Optimize based on user distribution
    val optimalPartitions = calculateOptimalPartitions(userCount, Runtime.getRuntime.availableProcessors())
    conf.set("spark.sql.shuffle.partitions", optimalPartitions.toString)
    
    // Enable broadcast join optimization for small reference data
    if (userCount < 10000) {
      conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")
    }
  }
  
  /**
   * Cleanup Spark resources properly.
   * 
   * @param spark SparkSession to cleanup
   */
  def cleanupSpark(spark: SparkSession): Unit = {
    try {
      if (spark != null && !spark.sparkContext.isStopped) {
        // Clear all cached data
        spark.catalog.clearCache()
        
        // Stop the session
        spark.sparkContext.setLogLevel("ERROR") // Reduce shutdown noise
        spark.stop()
      }
    } catch {
      case _: Exception => // Ignore cleanup failures
    }
  }
  
  /**
   * Gets current Spark configuration summary for diagnostics.
   * 
   * @param spark SparkSession to analyze
   * @return Configuration summary
   */
  def getConfigurationSummary(spark: SparkSession): Map[String, String] = {
    val conf = spark.conf
    Map(
      "master" -> spark.sparkContext.master,
      "app.name" -> spark.sparkContext.appName,
      "shuffle.partitions" -> conf.get("spark.sql.shuffle.partitions"),
      "driver.memory" -> conf.getOption("spark.driver.memory").getOrElse("default"),
      "executor.memory" -> conf.getOption("spark.executor.memory").getOrElse("default"),
      "serializer" -> conf.getOption("spark.serializer").getOrElse("default"),
      "adaptive.enabled" -> conf.getOption("spark.sql.adaptive.enabled").getOrElse("false"),
      "cores" -> Runtime.getRuntime.availableProcessors().toString
    )
  }
}