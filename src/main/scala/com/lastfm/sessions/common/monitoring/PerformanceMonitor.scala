package com.lastfm.sessions.common.monitoring

import com.lastfm.sessions.common.{ConfigurableConstants, ErrorMessages}
import com.lastfm.sessions.common.traits.MetricsCalculator
import org.slf4j.LoggerFactory
import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

/**
 * Performance monitoring and tracking traits.
 * 
 * Provides comprehensive performance tracking capabilities including:
 * - Execution time monitoring
 * - Memory usage tracking
 * - Throughput calculations
 * - Performance reporting
 * - Automatic warning detection
 * 
 * Design Principles:
 * - Low overhead monitoring (<1% performance impact)
 * - Thread-safe concurrent operations
 * - Automatic warning detection
 * - Comprehensive reporting
 * - Configurable thresholds
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait PerformanceMonitor extends MetricsCalculator {
  
  private val logger = LoggerFactory.getLogger(this.getClass)
  
  // Thread-safe metrics storage
  private val metrics = TrieMap[String, PerformanceMetric]()
  private val activeOperations = TrieMap[String, OperationContext]()
  
  /**
   * Tracks execution time and memory usage of a code block.
   * 
   * @param operationName Unique name for the operation
   * @param block Code block to monitor
   * @tparam T Return type of the block
   * @return Result of the block execution
   */
  def timeExecution[T](operationName: String)(block: => T): T = {
    val startTime = Instant.now()
    val startMemory = getCurrentMemoryUsage()
    val threadId = Thread.currentThread().getId
    val operationId = s"$operationName-$threadId-${System.nanoTime()}"
    
    // Track active operation
    activeOperations(operationId) = OperationContext(
      name = operationName,
      startTime = startTime,
      threadId = threadId
    )
    
    try {
      val result = block
      val duration = Duration.between(startTime, Instant.now())
      val endMemory = getCurrentMemoryUsage()
      val memoryUsed = Math.max(0, endMemory - startMemory)
      
      recordMetric(PerformanceMetric(
        operationName = operationName,
        startTime = startTime,
        duration = duration,
        memoryUsed = memoryUsed,
        success = true,
        threadId = threadId
      ))
      
      // Check for performance warnings
      checkPerformanceWarnings(operationName, duration.toMillis, memoryUsed)
      
      result
      
    } catch {
      case NonFatal(e) =>
        val duration = Duration.between(startTime, Instant.now())
        val endMemory = getCurrentMemoryUsage()
        val memoryUsed = Math.max(0, endMemory - startMemory)
        
        recordMetric(PerformanceMetric(
          operationName = operationName,
          startTime = startTime,
          duration = duration,
          memoryUsed = memoryUsed,
          success = false,
          error = Some(e.getMessage),
          threadId = threadId
        ))
        
        logger.error(s"Operation '$operationName' failed after ${duration.toMillis}ms", e)
        throw e
        
    } finally {
      activeOperations.remove(operationId)
    }
  }
  
  /**
   * Tracks throughput for batch operations.
   * 
   * @param operationName Operation identifier
   * @param itemCount Number of items processed
   * @param block Code block to monitor
   * @tparam T Return type
   * @return Result of block execution
   */
  def trackThroughput[T](
    operationName: String,
    itemCount: Long
  )(block: => T): T = {
    timeExecution(s"$operationName-throughput") {
      val result = block
      
      // Calculate and log throughput
      metrics.get(s"$operationName-throughput").foreach { metric =>
        val throughput = calculateThroughput(itemCount, metric.duration.toMillis)
        logger.info(f"$operationName throughput: ${formatThroughput(throughput, "items")}")
        
        // Check throughput warnings
        checkThroughputWarnings(operationName, throughput, itemCount)
      }
      
      result
    }
  }
  
  /**
   * Records a custom performance metric.
   * 
   * @param metric Performance metric to record
   */
  protected def recordMetric(metric: PerformanceMetric): Unit = {
    metrics(s"${metric.operationName}-${metric.threadId}-${metric.startTime.toEpochMilli}") = metric
    
    // Log performance information
    logMetric(metric)
    
    // Cleanup old metrics to prevent memory leaks
    cleanupOldMetrics()
  }
  
  /**
   * Gets comprehensive performance report.
   * 
   * @return Performance report with all collected metrics
   */
  def getPerformanceReport(): PerformanceReport = {
    val allMetrics = metrics.values.toList.sortBy(_.startTime)
    
    PerformanceReport(
      metrics = allMetrics,
      totalDuration = allMetrics.map(_.duration.toMillis).sum,
      totalMemoryUsed = allMetrics.map(_.memoryUsed).sum,
      slowestOperation = allMetrics.maxByOption(_.duration.toMillis),
      fastestOperation = allMetrics.minByOption(_.duration.toMillis),
      memoryIntensiveOperation = allMetrics.maxByOption(_.memoryUsed),
      successRate = {
        val total = allMetrics.size
        if (total == 0) 100.0
        else {
          val successful = allMetrics.count(_.success)
          (successful.toDouble / total) * 100.0
        }
      },
      averageExecutionTime = {
        val times = allMetrics.map(_.duration.toMillis)
        if (times.nonEmpty) times.sum.toDouble / times.size else 0.0
      },
      memoryEfficiency = {
        val allocated = Runtime.getRuntime.totalMemory()
        val used = allMetrics.map(_.memoryUsed).sum
        calculateMemoryEfficiency(used, allocated)
      }
    )
  }
  
  /**
   * Formats performance report as human-readable string.
   * 
   * @return Formatted performance report
   */
  def formatPerformanceReport(): String = {
    val report = getPerformanceReport()
    val sb = new StringBuilder
    
    sb.append("\n")
    sb.append("=" * 80).append("\n")
    sb.append("PERFORMANCE REPORT").append("\n")
    sb.append("=" * 80).append("\n")
    sb.append(f"Total Operations: ${report.metrics.size}%,d\n")
    sb.append(f"Total Duration: ${formatDuration(report.totalDuration)}\n")
    sb.append(f"Total Memory Used: ${formatMemorySize(report.totalMemoryUsed)}\n")
    sb.append(f"Success Rate: ${formatPercentage(report.successRate)}\n")
    sb.append(f"Average Execution Time: ${report.averageExecutionTime}%.2f ms\n")
    sb.append(f"Memory Efficiency: ${formatPercentage(report.memoryEfficiency * 100)}\n")
    sb.append("\n")
    
    // Slowest operations
    report.slowestOperation.foreach { op =>
      sb.append(f"Slowest Operation: ${op.operationName} (${formatDuration(op.duration.toMillis)})\n")
    }
    
    // Memory intensive operations
    report.memoryIntensiveOperation.foreach { op =>
      sb.append(f"Memory Intensive: ${op.operationName} (${formatMemorySize(op.memoryUsed)})\n")
    }
    
    // Failed operations
    val failedOps = report.metrics.filter(!_.success)
    if (failedOps.nonEmpty) {
      sb.append("\n").append("FAILED OPERATIONS:").append("\n")
      failedOps.foreach { op =>
        sb.append(f"  ✗ ${op.operationName}: ${op.error.getOrElse("Unknown error")}\n")
      }
    }
    
    // Performance by operation type
    val operationGroups = report.metrics.groupBy(_.operationName)
    if (operationGroups.size > 1) {
      sb.append("\n").append("OPERATION BREAKDOWN:").append("\n")
      operationGroups.toSeq.sortBy(_._1).foreach { case (opName, ops) =>
        val avgTime = ops.map(_.duration.toMillis).sum.toDouble / ops.size
        val totalMemory = ops.map(_.memoryUsed).sum
        val successRate = (ops.count(_.success).toDouble / ops.size) * 100
        
        sb.append(f"  $opName%s:\n")
        sb.append(f"    Count: ${ops.size}%,d\n")
        sb.append(f"    Avg Time: $avgTime%.2f ms\n")
        sb.append(f"    Total Memory: ${formatMemorySize(totalMemory)}\n")
        sb.append(f"    Success Rate: ${formatPercentage(successRate)}\n")
      }
    }
    
    sb.append("=" * 80).append("\n")
    sb.toString()
  }
  
  /**
   * Clears all performance metrics.
   */
  def clearMetrics(): Unit = {
    metrics.clear()
    activeOperations.clear()
  }
  
  /**
   * Gets current active operations.
   * 
   * @return List of currently running operations
   */
  def getActiveOperations(): List[OperationContext] = {
    activeOperations.values.toList.sortBy(_.startTime)
  }
  
  /**
   * Checks if any operations are running longer than expected.
   * 
   * @return List of potentially stuck operations
   */
  def getStuckOperations(): List[OperationContext] = {
    val now = Instant.now()
    val warningThreshold = ConfigurableConstants.Performance.WARNING_THRESHOLD_MS
    
    activeOperations.values.filter { op =>
      Duration.between(op.startTime, now).toMillis > warningThreshold
    }.toList
  }
  
  /**
   * Gets performance statistics for a specific operation.
   * 
   * @param operationName Name of the operation
   * @return Performance statistics
   */
  def getOperationStatistics(operationName: String): Option[OperationStatistics] = {
    val operationMetrics = metrics.values.filter(_.operationName == operationName).toList
    
    if (operationMetrics.nonEmpty) {
      val durations = operationMetrics.map(_.duration.toMillis.toDouble)
      val memories = operationMetrics.map(_.memoryUsed.toDouble)
      
      Some(OperationStatistics(
        operationName = operationName,
        executionCount = operationMetrics.size,
        successCount = operationMetrics.count(_.success),
        averageExecutionTime = durations.sum / durations.size,
        minExecutionTime = durations.min,
        maxExecutionTime = durations.max,
        standardDeviationTime = calculateStandardDeviation(durations.toArray),
        averageMemoryUsage = memories.sum / memories.size,
        maxMemoryUsage = memories.max,
        successRate = (operationMetrics.count(_.success).toDouble / operationMetrics.size) * 100
      ))
    } else {
      None
    }
  }
  
  /**
   * Exports metrics to structured format for external monitoring.
   * 
   * @return Map of metric data suitable for JSON serialization
   */
  def exportMetrics(): Map[String, Any] = {
    val report = getPerformanceReport()
    
    Map(
      "timestamp" -> Instant.now().toString,
      "totalOperations" -> report.metrics.size,
      "totalDurationMs" -> report.totalDuration,
      "totalMemoryBytes" -> report.totalMemoryUsed,
      "successRate" -> report.successRate,
      "averageExecutionTimeMs" -> report.averageExecutionTime,
      "memoryEfficiency" -> report.memoryEfficiency,
      "slowestOperation" -> report.slowestOperation.map(op => Map(
        "name" -> op.operationName,
        "durationMs" -> op.duration.toMillis,
        "memoryBytes" -> op.memoryUsed
      )),
      "activeOperations" -> getActiveOperations().map(op => Map(
        "name" -> op.name,
        "startTime" -> op.startTime.toString,
        "runningTimeMs" -> Duration.between(op.startTime, Instant.now()).toMillis
      )),
      "operationStatistics" -> metrics.values.groupBy(_.operationName).map { case (name, ops) =>
        name -> Map(
          "count" -> ops.size,
          "successRate" -> (ops.count(_.success).toDouble / ops.size) * 100,
          "averageTimeMs" -> ops.map(_.duration.toMillis).sum.toDouble / ops.size,
          "totalMemoryBytes" -> ops.map(_.memoryUsed).sum
        )
      }
    )
  }
  
  // Private helper methods
  
  private def getCurrentMemoryUsage(): Long = {
    val runtime = Runtime.getRuntime
    runtime.totalMemory() - runtime.freeMemory()
  }
  
  private def checkPerformanceWarnings(operationName: String, durationMs: Long, memoryUsed: Long): Unit = {
    val warningThreshold = ConfigurableConstants.Performance.WARNING_THRESHOLD_MS
    val errorThreshold = ConfigurableConstants.Performance.ERROR_THRESHOLD_MS
    
    if (durationMs > errorThreshold) {
      logger.error(ErrorMessages.Performance.slowOperation(operationName, durationMs, errorThreshold))
    } else if (durationMs > warningThreshold) {
      logger.warn(ErrorMessages.Performance.slowOperation(operationName, durationMs, warningThreshold))
    }
    
    // Memory warning (if using more than 100MB)
    val memoryWarningThreshold = 100 * 1024 * 1024 // 100MB
    if (memoryUsed > memoryWarningThreshold) {
      logger.warn(ErrorMessages.Performance.memoryWarning(operationName, memoryUsed, memoryWarningThreshold))
    }
  }
  
  private def checkThroughputWarnings(operationName: String, throughput: Double, itemCount: Long): Unit = {
    // Basic throughput warnings based on item count and operation type
    val expectedMinThroughput = if (itemCount > 1000000) 1000.0 else 100.0 // items/sec
    
    if (throughput < expectedMinThroughput) {
      logger.warn(ErrorMessages.Performance.throughputWarning(operationName, throughput, expectedMinThroughput))
    }
  }
  
  private def logMetric(metric: PerformanceMetric): Unit = {
    if (metric.success) {
      logger.debug(s"Operation '${metric.operationName}' completed in ${metric.duration.toMillis}ms " +
        s"using ${formatMemorySize(metric.memoryUsed)}")
    } else {
      logger.warn(s"Operation '${metric.operationName}' failed after ${metric.duration.toMillis}ms: " +
        s"${metric.error.getOrElse("Unknown error")}")
    }
  }
  
  private def cleanupOldMetrics(): Unit = {
    // Remove metrics older than 1 hour to prevent memory leaks
    val oneHourAgo = Instant.now().minusSeconds(3600)
    val oldKeys = metrics.filter(_._2.startTime.isBefore(oneHourAgo)).keys
    oldKeys.foreach(metrics.remove)
  }
}

/**
 * Data class representing a single performance metric.
 */
case class PerformanceMetric(
  operationName: String,
  startTime: Instant,
  duration: Duration,
  memoryUsed: Long,
  success: Boolean,
  error: Option[String] = None,
  threadId: Long = Thread.currentThread().getId,
  tags: Map[String, String] = Map.empty
)

/**
 * Data class for tracking active operations.
 */
case class OperationContext(
  name: String,
  startTime: Instant,
  threadId: Long
)

/**
 * Comprehensive performance report.
 */
case class PerformanceReport(
  metrics: List[PerformanceMetric],
  totalDuration: Long,
  totalMemoryUsed: Long,
  slowestOperation: Option[PerformanceMetric],
  fastestOperation: Option[PerformanceMetric],
  memoryIntensiveOperation: Option[PerformanceMetric],
  successRate: Double,
  averageExecutionTime: Double,
  memoryEfficiency: Double
)

/**
 * Statistical analysis for a specific operation.
 */
case class OperationStatistics(
  operationName: String,
  executionCount: Int,
  successCount: Int,
  averageExecutionTime: Double,
  minExecutionTime: Double,
  maxExecutionTime: Double,
  standardDeviationTime: Double,
  averageMemoryUsage: Double,
  maxMemoryUsage: Double,
  successRate: Double
)

