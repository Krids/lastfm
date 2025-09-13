package com.lastfm.sessions.common

/**
 * Centralized error message templates for consistent error reporting.
 * 
 * Provides standardized error messages across the entire application,
 * ensuring consistency in error reporting and user experience.
 * 
 * Design Principles:
 * - Consistent message formatting
 * - Clear, actionable error descriptions
 * - Contextual information for debugging
 * - User-friendly explanations
 * - Categorized by domain for easy maintenance
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object ErrorMessages {
  
  /**
   * Validation error messages
   */
  object Validation {
    val NULL_VALUE = "Value cannot be null"
    
    def emptyField(fieldName: String): String = 
      s"$fieldName cannot be empty"
    
    def invalidFormat(expectedFormat: String): String = 
      s"Invalid format. Expected: $expectedFormat"
    
    def outOfRange(fieldName: String, min: Double, max: Double): String = 
      s"$fieldName must be between $min and $max"
    
    def tooLong(fieldName: String, maxLength: Int): String = 
      s"$fieldName exceeds maximum length of $maxLength characters"
    
    def tooShort(fieldName: String, minLength: Int): String = 
      s"$fieldName must be at least $minLength characters"
    
    def invalidUserId(userId: String): String = 
      s"Invalid user ID format: '$userId'. Expected format: user_XXXXXX"
    
    def invalidTimestamp(timestamp: String): String = 
      s"Invalid timestamp format: '$timestamp'. Expected ISO 8601 format"
    
    def duplicateValue(fieldName: String, value: String): String = 
      s"Duplicate $fieldName found: '$value'"
    
    def invalidMBID(mbid: String): String =
      s"Invalid MusicBrainz ID format: '$mbid'. Expected UUID format"
    
    def invalidTrackKey(key: String): String =
      s"Invalid track key format: '$key'. Expected either MBID or 'artist — track'"
  }
  
  /**
   * Pipeline execution error messages
   */
  object Pipeline {
    def executionFailed(pipelineName: String, reason: String): String = 
      s"Pipeline '$pipelineName' failed: $reason"
    
    def stageFailure(stage: String, error: String): String = 
      s"Stage '$stage' failed with error: $error"
    
    def missingInput(inputPath: String): String = 
      s"Input file not found: $inputPath"
    
    def invalidConfiguration(param: String, value: String): String = 
      s"Invalid configuration: $param = '$value'"
    
    def qualityThresholdNotMet(actual: Double, required: Double): String = 
      s"Data quality ${BigDecimal(actual).setScale(2, BigDecimal.RoundingMode.HALF_UP)}% below required threshold ${BigDecimal(required).setScale(2, BigDecimal.RoundingMode.HALF_UP)}%"
    
    def partitioningError(expected: Int, actual: Int): String = 
      s"Partitioning mismatch: expected $expected partitions, got $actual"
    
    def dependencyNotMet(pipeline: String, dependency: String): String =
      s"Pipeline '$pipeline' cannot run: dependency '$dependency' not satisfied"
    
    def configurationValidationFailed(errors: List[String]): String = {
      val errorList = errors.map(e => s"  - $e").mkString("\n")
      s"Configuration validation failed:\n$errorList"
    }
  }
  
  /**
   * File I/O error messages
   */
  object IO {
    def fileNotFound(path: String): String = 
      s"File not found: $path"
    
    def directoryNotFound(path: String): String = 
      s"Directory not found: $path"
    
    def readError(path: String, error: String): String = 
      s"Failed to read from '$path': $error"
    
    def writeError(path: String, error: String): String = 
      s"Failed to write to '$path': $error"
    
    def permissionDenied(path: String): String = 
      s"Permission denied accessing: $path"
    
    def diskSpaceError(required: Long, available: Long): String = 
      s"Insufficient disk space. Required: ${required / 1024 / 1024}MB, Available: ${available / 1024 / 1024}MB"
    
    def corruptedFile(path: String, reason: String): String =
      s"File '$path' appears to be corrupted: $reason"
    
    def unsupportedFormat(path: String, expectedFormat: String): String =
      s"Unsupported file format for '$path'. Expected: $expectedFormat"
  }
  
  /**
   * Spark-specific error messages
   */
  object Spark {
    def sessionCreationFailed(error: String): String = 
      s"Failed to create Spark session: $error"
    
    def outOfMemory(operation: String): String = 
      s"Out of memory during operation: $operation"
    
    def shuffleError(operation: String): String = 
      s"Shuffle operation failed: $operation"
    
    def broadcastError(size: Long): String = 
      s"Broadcast variable too large: ${size / 1024 / 1024}MB"
    
    def partitionSkew(maxRecords: Long, avgRecords: Long): String = 
      s"Severe partition skew detected. Max: $maxRecords, Avg: $avgRecords"
    
    def executorLost(executorId: String): String =
      s"Executor lost during processing: $executorId"
    
    def driverHeapError(usedMemory: Long, totalMemory: Long): String =
      s"Driver heap space exhausted: ${usedMemory / 1024 / 1024}MB used of ${totalMemory / 1024 / 1024}MB"
    
    def cacheEvictionError(reason: String): String =
      s"Cache eviction failed: $reason"
  }
  
  /**
   * Session analysis specific error messages
   */
  object Session {
    def invalidSessionGap(gap: Int): String = 
      s"Invalid session gap: $gap minutes. Must be between 1 and ${Constants.Limits.MAX_SESSION_DURATION_MINUTES}"
    
    def emptySession(userId: String): String = 
      s"Empty session detected for user: $userId"
    
    def chronologicalOrder(userId: String): String = 
      s"Events not in chronological order for user: $userId"
    
    def sessionOverflow(count: Int, limit: Int): String = 
      s"Session count $count exceeds limit of $limit"
    
    def sessionBoundaryError(userId: String, timestamp: String): String =
      s"Invalid session boundary for user '$userId' at timestamp '$timestamp'"
    
    def duplicateEvents(userId: String, timestamp: String): String =
      s"Duplicate events found for user '$userId' at timestamp '$timestamp'"
    
    def timestampParsingError(timestamp: String, format: String): String =
      s"Failed to parse timestamp '$timestamp'. Expected format: $format"
  }
  
  /**
   * Ranking pipeline error messages
   */
  object Ranking {
    def insufficientSessions(actual: Int, required: Int): String = 
      s"Insufficient sessions for ranking. Found: $actual, Required: $required"
    
    def insufficientTracks(actual: Int, required: Int): String = 
      s"Insufficient tracks for top songs. Found: $actual, Required: $required"
    
    def tieBreakingFailed(reason: String): String = 
      s"Failed to break ranking tie: $reason"
    
    def rankingCalculationError(operation: String, error: String): String =
      s"Ranking calculation failed during '$operation': $error"
    
    def outputGenerationError(format: String, error: String): String =
      s"Failed to generate $format output: $error"
  }
  
  /**
   * Data quality error messages
   */
  object Quality {
    def qualityCheckFailed(metric: String, actual: Double, expected: Double): String =
      f"Quality check failed: $metric is $actual%.2f%%, expected >= $expected%.2f%%"
    
    def suspiciousDataPattern(pattern: String, count: Long): String =
      s"Suspicious data pattern detected: $pattern ($count occurrences)"
    
    def dataIntegrityError(field: String, issue: String): String =
      s"Data integrity error in field '$field': $issue"
    
    def validationRuleFailed(rule: String, value: String): String =
      s"Validation rule '$rule' failed for value: '$value'"
    
    def qualityReportGenerationFailed(error: String): String =
      s"Failed to generate quality report: $error"
  }
  
  /**
   * Performance monitoring error messages
   */
  object Performance {
    def monitoringError(operation: String, error: String): String =
      s"Performance monitoring failed for operation '$operation': $error"
    
    def slowOperation(operation: String, duration: Long, threshold: Long): String =
      s"Slow operation detected: '$operation' took ${duration}ms (threshold: ${threshold}ms)"
    
    def memoryWarning(operation: String, used: Long, limit: Long): String =
      s"High memory usage in operation '$operation': ${used / 1024 / 1024}MB used (limit: ${limit / 1024 / 1024}MB)"
    
    def throughputWarning(operation: String, actual: Double, expected: Double): String =
      f"Low throughput in operation '$operation': $actual%.2f items/sec (expected: $expected%.2f)"
  }
  
  /**
   * Formats error with context information for debugging
   */
  def withContext(baseError: String, context: Map[String, Any]): String = {
    if (context.isEmpty) {
      baseError
    } else {
      val contextStr = context.map { case (k, v) => s"  $k: $v" }.mkString("\n")
      s"$baseError\nContext:\n$contextStr"
    }
  }
  
  /**
   * Creates user-friendly error message with troubleshooting hints
   */
  def userFriendly(technicalError: String): String = {
    technicalError match {
      case e if e.contains("OutOfMemory") || e.contains("heap space") => 
        "The application ran out of memory. Try increasing memory allocation with -Xmx flag or reducing data batch size."
      
      case e if e.contains("FileNotFound") => 
        "Input file not found. Please ensure the data files are in the correct location and have proper permissions."
      
      case e if e.contains("Permission") => 
        "Permission denied. Please check file permissions and ensure the application has read/write access to the required directories."
      
      case e if e.contains("quality") && e.contains("threshold") => 
        "Data quality is below acceptable levels. Please review the data cleaning report for details and consider data source improvements."
      
      case e if e.contains("partition") && e.contains("skew") =>
        "Data is unevenly distributed across partitions. Consider repartitioning the data or adjusting partition strategy."
      
      case e if e.contains("session") && e.contains("gap") =>
        "Invalid session gap configuration. Session gap must be between 1 and 1440 minutes (24 hours)."
      
      case e if e.contains("Spark") && e.contains("connection") =>
        "Spark connection error. Check if Spark is properly configured and running."
      
      case e if e.contains("timeout") =>
        "Operation timed out. This might indicate network issues or insufficient resources. Try increasing timeout values or reducing data size."
      
      case _ => 
        s"An error occurred: $technicalError\nPlease check the logs for more details or contact support if the issue persists."
    }
  }
  
  /**
   * Creates structured error for logging and monitoring systems
   */
  def structured(
    error: String,
    category: String = "GENERAL",
    severity: String = "ERROR",
    component: String = "UNKNOWN",
    context: Map[String, Any] = Map.empty
  ): String = {
    val baseStructure = Map(
      "error" -> error,
      "category" -> category,
      "severity" -> severity,
      "component" -> component,
      "timestamp" -> java.time.Instant.now().toString
    )
    
    val fullContext = baseStructure ++ context
    fullContext.map { case (k, v) => s"$k=$v" }.mkString(" ")
  }
  
  /**
   * Common error categories for structured logging
   */
  object Categories {
    val VALIDATION = "VALIDATION"
    val PIPELINE = "PIPELINE"
    val IO = "IO"
    val SPARK = "SPARK"
    val SESSION = "SESSION"
    val RANKING = "RANKING"
    val QUALITY = "QUALITY"
    val PERFORMANCE = "PERFORMANCE"
    val CONFIGURATION = "CONFIGURATION"
  }
  
  /**
   * Common severity levels
   */
  object Severity {
    val INFO = "INFO"
    val WARN = "WARN"
    val ERROR = "ERROR"
    val FATAL = "FATAL"
  }
}