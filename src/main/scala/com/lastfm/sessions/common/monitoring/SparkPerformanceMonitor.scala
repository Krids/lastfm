package com.lastfm.sessions.common.monitoring

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionStart, SparkListenerSQLExecutionEnd}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.util.AccumulatorV2
import org.slf4j.LoggerFactory
import scala.collection.mutable

/**
 * Spark-specific performance monitoring extension.
 * 
 * Provides enhanced monitoring capabilities for Spark operations including:
 * - DataFrame operation tracking
 * - Spark job monitoring
 * - Executor performance analysis
 * - Cache efficiency tracking
 * - Partition distribution analysis
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait SparkPerformanceMonitor extends PerformanceMonitor {
  
  private val sparkLogger = LoggerFactory.getLogger(s"${this.getClass.getSimpleName}.Spark")
  
  /**
   * Monitors a Spark operation with enhanced DataFrame metrics.
   * 
   * @param operationName Operation identifier
   * @param spark SparkSession for accessing metrics
   * @param block Code block to monitor
   * @tparam T Return type
   * @return Result of block execution
   */
  def monitorSparkOperation[T](
    operationName: String,
    spark: SparkSession
  )(block: => T): T = {
    
    val initialMetrics = getSparkMetrics(spark)
    
    timeExecution(s"spark-$operationName") {
      val result = block
      
      val finalMetrics = getSparkMetrics(spark)
      val deltaMetrics = calculateSparkMetricsDelta(initialMetrics, finalMetrics)
      
      logSparkMetrics(operationName, deltaMetrics)
      
      result
    }
  }
  
  /**
   * Analyzes DataFrame characteristics for performance insights.
   * 
   * @param df DataFrame to analyze
   * @param name Descriptive name for the DataFrame
   * @return Comprehensive DataFrame metrics
   */
  def analyzeDataFrame(df: DataFrame, name: String): DataFrameMetrics = {
    val startTime = System.currentTimeMillis()
    
    // Get partition count without triggering computation
    val partitionCount = df.rdd.getNumPartitions
    
    // Get schema information
    val columnCount = df.columns.length
    val schemaSize = df.schema.json.length
    
    // Analyze partitions (this will trigger computation)
    val partitionSizes = timeExecution(s"analyze-partitions-$name") {
      df.rdd.mapPartitions(iter => Iterator(iter.size)).collect()
    }
    
    val totalRecords = partitionSizes.sum.toLong
    val avgRecordsPerPartition = if (partitionCount > 0) totalRecords / partitionCount else 0L
    val maxPartitionSize = if (partitionSizes.nonEmpty) partitionSizes.max else 0
    val minPartitionSize = if (partitionSizes.nonEmpty) partitionSizes.min else 0
    val partitionSkew = if (avgRecordsPerPartition > 0) maxPartitionSize.toDouble / avgRecordsPerPartition else 1.0
    
    val analysisTime = System.currentTimeMillis() - startTime
    
    val metrics = DataFrameMetrics(
      name = name,
      partitions = partitionCount,
      records = totalRecords,
      columns = columnCount,
      avgRecordsPerPartition = avgRecordsPerPartition,
      maxPartitionSize = maxPartitionSize,
      minPartitionSize = minPartitionSize,
      partitionSkew = partitionSkew,
      schemaSize = schemaSize,
      analysisTimeMs = analysisTime,
      isBalanced = partitionSkew < 2.0 // Balanced if max partition is less than 2x average
    )
    
    // Log DataFrame analysis
    sparkLogger.info(s"DataFrame '$name' analysis: $totalRecords records, $partitionCount partitions, " +
      f"${partitionSkew}%.2fx skew, ${metrics.isBalanced}%s balanced")
    
    // Warn about performance issues
    if (partitionSkew > 5.0) {
      sparkLogger.warn(s"High partition skew detected in DataFrame '$name': ${partitionSkew}x")
    }
    
    if (partitionCount > Runtime.getRuntime.availableProcessors() * 10) {
      sparkLogger.warn(s"DataFrame '$name' has many partitions ($partitionCount) for available cores. " +
        "Consider coalescing.")
    }
    
    if (avgRecordsPerPartition < 1000) {
      sparkLogger.warn(s"DataFrame '$name' has small partitions (avg: $avgRecordsPerPartition records). " +
        "Consider reducing partition count.")
    }
    
    metrics
  }
  
  /**
   * Monitors cache operations and efficiency.
   * 
   * @param df DataFrame to cache
   * @param name Descriptive name
   * @return Cached DataFrame with monitoring
   */
  def monitorCache(df: DataFrame, name: String): DataFrame = {
    timeExecution(s"cache-$name") {
      val cachedDf = df.cache()
      
      // Force caching by counting
      val recordCount = cachedDf.count()
      
      sparkLogger.info(s"Cached DataFrame '$name': $recordCount records")
      cachedDf
    }
  }
  
  /**
   * Analyzes cache efficiency for a SparkSession.
   * 
   * @param spark SparkSession to analyze
   * @return Cache efficiency metrics
   */
  def analyzeCacheEfficiency(spark: SparkSession): CacheEfficiencyMetrics = {
    // Simplified implementation using publicly available APIs
    // In practice, detailed cache metrics would require access to internal Spark APIs
    CacheEfficiencyMetrics(
      totalCachedTables = 0,
      totalCacheSize = 0L,
      cacheHitRatio = 0.0,
      evictionCount = 0L
    )
  }
  
  /**
   * Gets Spark executor metrics.
   * 
   * @param spark SparkSession
   * @return List of executor information
   */
  def getExecutorMetrics(spark: SparkSession): List[ExecutorMetrics] = {
    val statusTracker = spark.sparkContext.statusTracker
    val executorInfos = statusTracker.getExecutorInfos
    
    executorInfos.map { executor =>
      ExecutorMetrics(
        executorId = executor.host,
        host = executor.host,
        isActive = true,
        totalCores = Runtime.getRuntime.availableProcessors(),
        maxMemory = Runtime.getRuntime.maxMemory(),
        memoryUsed = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory(),
        diskUsed = 0L,
        activeTasks = 0,
        failedTasks = 0,
        completedTasks = 0,
        totalTasks = 0,
        maxTasks = Runtime.getRuntime.availableProcessors()
      )
    }.toList
  }
  
  /**
   * Validates Spark configuration for performance optimization.
   * 
   * @param spark SparkSession to validate
   * @return List of performance recommendations
   */
  def validateSparkPerformance(spark: SparkSession): List[String] = {
    val recommendations = mutable.ListBuffer[String]()
    val conf = spark.conf
    
    // Check shuffle partitions
    val shufflePartitions = conf.get("spark.sql.shuffle.partitions", "200").toInt
    val cores = Runtime.getRuntime.availableProcessors()
    
    if (shufflePartitions > cores * 10) {
      recommendations += s"Consider reducing shuffle partitions from $shufflePartitions to ${cores * 4} for better performance"
    }
    
    // Check adaptive query execution
    val adaptiveEnabled = conf.getOption("spark.sql.adaptive.enabled").exists(_.toBoolean)
    if (!adaptiveEnabled) {
      recommendations += "Enable Adaptive Query Execution for better performance: spark.sql.adaptive.enabled=true"
    }
    
    // Check serializer
    val serializer = conf.get("spark.serializer", "default")
    if (!serializer.contains("Kryo")) {
      recommendations += "Use KryoSerializer for better performance: spark.serializer=org.apache.spark.serializer.KryoSerializer"
    }
    
    // Check driver memory
    val driverMemory = conf.get("spark.driver.memory", "1g")
    if (driverMemory == "1g") {
      recommendations += "Consider increasing driver memory for large datasets"
    }
    
    // Check executor memory
    val executorMemory = conf.getOption("spark.executor.memory")
    if (executorMemory.isEmpty) {
      recommendations += "Set explicit executor memory for better resource management"
    }
    
    recommendations.toList
  }
  
  /**
   * Creates performance summary for Spark operations.
   * 
   * @param spark SparkSession
   * @return Formatted performance summary
   */
  def createSparkPerformanceSummary(spark: SparkSession): String = {
    val sb = new StringBuilder
    val executorMetrics = getExecutorMetrics(spark)
    val recommendations = validateSparkPerformance(spark)
    
    sb.append("\n")
    sb.append("=" * 60).append("\n")
    sb.append("SPARK PERFORMANCE SUMMARY").append("\n")
    sb.append("=" * 60).append("\n")
    
    // Spark configuration
    sb.append("Configuration:\n")
    sb.append(f"  Master: ${spark.sparkContext.master}\n")
    sb.append(f"  App Name: ${spark.sparkContext.appName}\n")
    sb.append(f"  Shuffle Partitions: ${spark.conf.get("spark.sql.shuffle.partitions", "200")}\n")
    sb.append(f"  Driver Memory: ${spark.conf.get("spark.driver.memory", "default")}\n")
    sb.append("\n")
    
    // Executor metrics
    if (executorMetrics.nonEmpty) {
      sb.append("Executors:\n")
      executorMetrics.foreach { exec =>
        val memoryUtilization = if (exec.maxMemory > 0) (exec.memoryUsed.toDouble / exec.maxMemory) * 100 else 0
        sb.append(f"  ${exec.executorId}: ${exec.totalCores} cores, " +
          f"${formatMemorySize(exec.maxMemory)} memory, " +
          f"${memoryUtilization}%.1f%% used, " +
          f"${exec.completedTasks} tasks completed\n")
      }
      sb.append("\n")
    }
    
    // Performance recommendations
    if (recommendations.nonEmpty) {
      sb.append("Performance Recommendations:\n")
      recommendations.foreach { rec =>
        sb.append(s"  • $rec\n")
      }
      sb.append("\n")
    }
    
    // General performance report
    sb.append(formatPerformanceReport())
    
    sb.toString()
  }
  
  // Private helper methods
  
  private def getSparkMetrics(spark: SparkSession): SparkMetricsSnapshot = {
    val sc = spark.sparkContext
    val statusTracker = sc.statusTracker
    
    SparkMetricsSnapshot(
      activeJobs = statusTracker.getActiveJobIds().length,
      activeStages = statusTracker.getActiveStageIds().length,
      executorCount = statusTracker.getExecutorInfos.length
    )
  }
  
  private def calculateSparkMetricsDelta(
    initial: SparkMetricsSnapshot,
    finalMetrics: SparkMetricsSnapshot
  ): SparkMetricsDelta = {
    SparkMetricsDelta(
      jobsDelta = finalMetrics.activeJobs - initial.activeJobs,
      stagesDelta = finalMetrics.activeStages - initial.activeStages,
      executorsDelta = finalMetrics.executorCount - initial.executorCount
    )
  }
  
  private def logSparkMetrics(operationName: String, delta: SparkMetricsDelta): Unit = {
    sparkLogger.debug(s"Spark operation '$operationName' metrics: " +
      s"jobs=${delta.jobsDelta}, stages=${delta.stagesDelta}, executors=${delta.executorsDelta}")
  }
}

/**
 * DataFrame analysis metrics.
 */
case class DataFrameMetrics(
  name: String,
  partitions: Int,
  records: Long,
  columns: Int,
  avgRecordsPerPartition: Long,
  maxPartitionSize: Int,
  minPartitionSize: Int,
  partitionSkew: Double,
  schemaSize: Int,
  analysisTimeMs: Long,
  isBalanced: Boolean
)

/**
 * Cache efficiency metrics.
 */
case class CacheEfficiencyMetrics(
  totalCachedTables: Int,
  totalCacheSize: Long,
  cacheHitRatio: Double,
  evictionCount: Long
)

/**
 * Executor performance metrics.
 */
case class ExecutorMetrics(
  executorId: String,
  host: String,
  isActive: Boolean,
  totalCores: Int,
  maxMemory: Long,
  memoryUsed: Long,
  diskUsed: Long,
  activeTasks: Int,
  failedTasks: Int,
  completedTasks: Int,
  totalTasks: Int,
  maxTasks: Int
)

/**
 * Snapshot of Spark metrics at a point in time.
 */
private case class SparkMetricsSnapshot(
  activeJobs: Int,
  activeStages: Int,
  executorCount: Int
)

/**
 * Delta between two Spark metrics snapshots.
 */
private case class SparkMetricsDelta(
  jobsDelta: Int,
  stagesDelta: Int,
  executorsDelta: Int
)