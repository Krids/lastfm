package com.lastfm.sessions.common.traits

import com.lastfm.sessions.common.{Constants, ConfigurableConstants}
import java.time.Duration

/**
 * Trait providing common metrics calculation patterns.
 * 
 * Encapsulates reusable mathematical and statistical calculations
 * used throughout the LastFM session analysis pipeline.
 * 
 * Design Principles:
 * - Consistent calculation methods
 * - Safe division with zero checks
 * - Business-aware formatting
 * - Performance-optimized computations
 * - Type-safe numeric operations
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait MetricsCalculator {
  
  /**
   * Calculates data quality score as percentage.
   * 
   * @param totalRecords Total number of records processed
   * @param validRecords Number of records that passed validation
   * @return Quality score as percentage (0.0 to 100.0)
   */
  def calculateQualityScore(totalRecords: Long, validRecords: Long): Double = {
    if (totalRecords == 0) {
      100.0 // No records means perfect quality
    } else {
      (validRecords.toDouble / totalRecords) * 100.0
    }
  }
  
  /**
   * Calculates rejection rate as percentage.
   * 
   * @param totalRecords Total number of records
   * @param rejectedRecords Number of rejected records
   * @return Rejection rate as percentage (0.0 to 100.0)
   */
  def calculateRejectionRate(totalRecords: Long, rejectedRecords: Long): Double = {
    if (totalRecords == 0) {
      0.0
    } else {
      (rejectedRecords.toDouble / totalRecords) * 100.0
    }
  }
  
  /**
   * Determines if data quality meets session analysis requirements.
   * 
   * @param qualityScore Overall quality score percentage
   * @param trackIdCoverage Track ID coverage percentage
   * @return True if quality is sufficient for session analysis
   */
  def isSessionAnalysisReady(qualityScore: Double, trackIdCoverage: Double): Boolean = {
    qualityScore >= ConfigurableConstants.DataQuality.SESSION_ANALYSIS_MIN_QUALITY.value && 
    trackIdCoverage >= ConfigurableConstants.DataQuality.MIN_TRACK_ID_COVERAGE.value
  }
  
  /**
   * Determines if data quality meets production requirements.
   * 
   * @param qualityScore Overall quality score percentage
   * @return True if quality is sufficient for production
   */
  def isProductionReady(qualityScore: Double): Boolean = {
    qualityScore >= ConfigurableConstants.DataQuality.PRODUCTION_MIN_QUALITY.value
  }
  
  /**
   * Calculates throughput in items per second.
   * 
   * @param itemsProcessed Number of items processed
   * @param timeMillis Processing time in milliseconds
   * @return Throughput in items per second
   */
  def calculateThroughput(itemsProcessed: Long, timeMillis: Long): Double = {
    if (timeMillis == 0) {
      0.0
    } else {
      (itemsProcessed.toDouble / timeMillis) * 1000.0
    }
  }
  
  /**
   * Calculates average session length.
   * 
   * @param totalTracks Total tracks across all sessions
   * @param sessionCount Number of sessions
   * @return Average tracks per session
   */
  def calculateAverageSessionLength(totalTracks: Long, sessionCount: Long): Double = {
    if (sessionCount == 0) {
      0.0
    } else {
      totalTracks.toDouble / sessionCount
    }
  }
  
  /**
   * Calculates tracks per user ratio.
   * 
   * @param totalTracks Total tracks
   * @param userCount Number of unique users
   * @return Average tracks per user
   */
  def calculateTracksPerUser(totalTracks: Long, userCount: Long): Double = {
    if (userCount == 0) {
      0.0
    } else {
      totalTracks.toDouble / userCount
    }
  }
  
  /**
   * Calculates sessions per user ratio.
   * 
   * @param totalSessions Total sessions
   * @param userCount Number of unique users
   * @return Average sessions per user
   */
  def calculateSessionsPerUser(totalSessions: Long, userCount: Long): Double = {
    if (userCount == 0) {
      0.0
    } else {
      totalSessions.toDouble / userCount
    }
  }
  
  /**
   * Calculates track ID coverage percentage.
   * 
   * @param recordsWithTrackId Records that have track MBID
   * @param totalRecords Total records
   * @return Coverage percentage (0.0 to 100.0)
   */
  def calculateTrackIdCoverage(recordsWithTrackId: Long, totalRecords: Long): Double = {
    if (totalRecords == 0) {
      0.0
    } else {
      (recordsWithTrackId.toDouble / totalRecords) * 100.0
    }
  }
  
  /**
   * Calculates suspicious user ratio.
   * 
   * @param suspiciousUsers Number of users with suspicious patterns
   * @param totalUsers Total number of users
   * @return Suspicious user ratio as percentage
   */
  def calculateSuspiciousUserRatio(suspiciousUsers: Long, totalUsers: Long): Double = {
    if (totalUsers == 0) {
      0.0
    } else {
      (suspiciousUsers.toDouble / totalUsers) * 100.0
    }
  }
  
  /**
   * Calculates partition balance score.
   * 
   * @param maxPartitionSize Largest partition size
   * @param avgPartitionSize Average partition size
   * @return Balance score (1.0 = perfect balance, higher = more skewed)
   */
  def calculatePartitionBalance(maxPartitionSize: Long, avgPartitionSize: Double): Double = {
    if (avgPartitionSize == 0.0) {
      1.0
    } else {
      maxPartitionSize / avgPartitionSize
    }
  }
  
  /**
   * Calculates memory efficiency ratio.
   * 
   * @param usedMemory Memory actually used (bytes)
   * @param allocatedMemory Memory allocated (bytes)
   * @return Efficiency ratio (0.0 to 1.0, higher is better)
   */
  def calculateMemoryEfficiency(usedMemory: Long, allocatedMemory: Long): Double = {
    if (allocatedMemory == 0) {
      0.0
    } else {
      Math.min(1.0, usedMemory.toDouble / allocatedMemory)
    }
  }
  
  /**
   * Calculates cache hit ratio.
   * 
   * @param cacheHits Number of cache hits
   * @param totalRequests Total number of requests
   * @return Hit ratio as percentage (0.0 to 100.0)
   */
  def calculateCacheHitRatio(cacheHits: Long, totalRequests: Long): Double = {
    if (totalRequests == 0) {
      0.0
    } else {
      (cacheHits.toDouble / totalRequests) * 100.0
    }
  }
  
  /**
   * Formats percentage value for display.
   * 
   * @param value Percentage value
   * @param decimals Number of decimal places
   * @return Formatted percentage string
   */
  def formatPercentage(value: Double, decimals: Int = 2): String = {
    s"${BigDecimal(value).setScale(decimals, BigDecimal.RoundingMode.HALF_UP)}%"
  }
  
  /**
   * Formats duration in human-readable format.
   * 
   * @param millis Duration in milliseconds
   * @return Human-readable duration string
   */
  def formatDuration(millis: Long): String = {
    val seconds = millis / 1000
    val minutes = seconds / 60
    val hours = minutes / 60
    val days = hours / 24
    
    if (days > 0) {
      s"${days}d ${hours % 24}h ${minutes % 60}m"
    } else if (hours > 0) {
      s"${hours}h ${minutes % 60}m"
    } else if (minutes > 0) {
      s"${minutes}m ${seconds % 60}s"
    } else if (seconds > 0) {
      s"${seconds}s"
    } else {
      s"${millis}ms"
    }
  }
  
  /**
   * Formats memory size in human-readable format.
   * 
   * @param bytes Memory size in bytes
   * @return Human-readable memory string
   */
  def formatMemorySize(bytes: Long): String = {
    val units = Array("B", "KB", "MB", "GB", "TB")
    var size = bytes.toDouble
    var unitIndex = 0
    
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024
      unitIndex += 1
    }
    
    if (unitIndex == 0) {
      s"${size.toInt} ${units(unitIndex)}"
    } else {
      s"${BigDecimal(size).setScale(2, BigDecimal.RoundingMode.HALF_UP)} ${units(unitIndex)}"
    }
  }
  
  /**
   * Formats throughput for display.
   * 
   * @param throughput Throughput value
   * @param unit Unit of measurement (e.g., "records", "tracks")
   * @return Formatted throughput string
   */
  def formatThroughput(throughput: Double, unit: String = "items"): String = {
    if (throughput >= 1000000) {
      s"${BigDecimal(throughput / 1000000).setScale(2, BigDecimal.RoundingMode.HALF_UP)}M $unit/sec"
    } else if (throughput >= 1000) {
      s"${BigDecimal(throughput / 1000).setScale(2, BigDecimal.RoundingMode.HALF_UP)}K $unit/sec"
    } else {
      s"${BigDecimal(throughput).setScale(2, BigDecimal.RoundingMode.HALF_UP)} $unit/sec"
    }
  }
  
  /**
   * Calculates percentile value from sorted data.
   * 
   * @param sortedValues Sorted array of values
   * @param percentile Percentile to calculate (0-100)
   * @return Percentile value
   */
  def calculatePercentile(sortedValues: Array[Double], percentile: Double): Double = {
    require(percentile >= 0 && percentile <= 100, "Percentile must be between 0 and 100")
    require(sortedValues.nonEmpty, "Values array cannot be empty")
    
    if (sortedValues.length == 1) {
      sortedValues(0)
    } else {
      val index = (percentile / 100.0) * (sortedValues.length - 1)
      val lowerIndex = index.toInt
      val upperIndex = Math.min(lowerIndex + 1, sortedValues.length - 1)
      val weight = index - lowerIndex
      
      sortedValues(lowerIndex) * (1 - weight) + sortedValues(upperIndex) * weight
    }
  }
  
  /**
   * Calculates standard deviation.
   * 
   * @param values Array of values
   * @return Standard deviation
   */
  def calculateStandardDeviation(values: Array[Double]): Double = {
    if (values.isEmpty) {
      0.0
    } else if (values.length == 1) {
      0.0
    } else {
      val mean = values.sum / values.length
      val squaredDifferences = values.map(value => Math.pow(value - mean, 2))
      val variance = squaredDifferences.sum / values.length
      Math.sqrt(variance)
    }
  }
  
  /**
   * Calculates coefficient of variation (relative standard deviation).
   * 
   * @param values Array of values
   * @return Coefficient of variation as percentage
   */
  def calculateCoefficientOfVariation(values: Array[Double]): Double = {
    if (values.isEmpty) {
      0.0
    } else {
      val mean = values.sum / values.length
      if (mean == 0) {
        0.0
      } else {
        val stdDev = calculateStandardDeviation(values)
        (stdDev / mean) * 100.0
      }
    }
  }
  
  /**
   * Determines engagement level based on average session length.
   * 
   * @param averageSessionLength Average tracks per session
   * @return Engagement level description
   */
  def calculateEngagementLevel(averageSessionLength: Double): String = {
    if (averageSessionLength >= 50.0) {
      "High"
    } else if (averageSessionLength >= 20.0) {
      "Moderate"
    } else {
      "Low"
    }
  }
  
  /**
   * Calculates ecosystem health score based on multiple factors.
   * 
   * @param qualityScore Data quality percentage
   * @param averageSessionLength Average session length
   * @param userEngagement User engagement ratio
   * @return Health score (0.0 to 100.0)
   */
  def calculateEcosystemHealth(
    qualityScore: Double, 
    averageSessionLength: Double, 
    userEngagement: Double
  ): Double = {
    // Weighted combination of factors
    val qualityWeight = 0.5
    val sessionWeight = 0.3
    val engagementWeight = 0.2
    
    val normalizedSessionLength = Math.min(100.0, averageSessionLength * 2) // Normalize to 0-100
    val normalizedEngagement = Math.min(100.0, userEngagement * 10) // Normalize to 0-100
    
    (qualityScore * qualityWeight) + 
    (normalizedSessionLength * sessionWeight) + 
    (normalizedEngagement * engagementWeight)
  }
  
  /**
   * Formats number with appropriate scale (K, M, B).
   * 
   * @param number Number to format
   * @param decimals Number of decimal places
   * @return Formatted number string
   */
  def formatNumber(number: Long, decimals: Int = 1): String = {
    if (number >= 1000000000) {
      s"${BigDecimal(number / 1000000000.0).setScale(decimals, BigDecimal.RoundingMode.HALF_UP)}B"
    } else if (number >= 1000000) {
      s"${BigDecimal(number / 1000000.0).setScale(decimals, BigDecimal.RoundingMode.HALF_UP)}M"
    } else if (number >= 1000) {
      s"${BigDecimal(number / 1000.0).setScale(decimals, BigDecimal.RoundingMode.HALF_UP)}K"
    } else {
      number.toString
    }
  }
  
  /**
   * Calculates growth rate between two values.
   * 
   * @param oldValue Previous value
   * @param newValue Current value
   * @return Growth rate as percentage (positive = growth, negative = decline)
   */
  def calculateGrowthRate(oldValue: Double, newValue: Double): Double = {
    if (oldValue == 0) {
      if (newValue == 0) 0.0 else 100.0
    } else {
      ((newValue - oldValue) / oldValue) * 100.0
    }
  }
}