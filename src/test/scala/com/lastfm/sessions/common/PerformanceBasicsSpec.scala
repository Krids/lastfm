package com.lastfm.sessions.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.lastfm.sessions.common.monitoring.PerformanceMonitor
import com.lastfm.sessions.common.traits.MetricsCalculator

/**
 * Tests for basic performance monitoring functionality.
 * 
 * Simplified performance tests focusing on core calculations.
 */
class PerformanceBasicsSpec extends AnyFlatSpec with Matchers with PerformanceMonitor with MetricsCalculator {
  
  "MetricsCalculator" should "calculate quality scores correctly" in {
    calculateQualityScore(1000, 950) should be(95.0)
    calculateQualityScore(1000, 1000) should be(100.0)
    calculateQualityScore(1000, 0) should be(0.0)
    calculateQualityScore(0, 0) should be(100.0) // No records = perfect quality
  }
  
  it should "calculate rejection rates correctly" in {
    calculateRejectionRate(1000, 50) should be(5.0)
    calculateRejectionRate(1000, 0) should be(0.0)
    calculateRejectionRate(0, 0) should be(0.0)
  }
  
  it should "determine session analysis readiness correctly" in {
    isSessionAnalysisReady(99.5, 90.0) should be(true)
    isSessionAnalysisReady(98.0, 90.0) should be(false) // Below quality threshold
    isSessionAnalysisReady(99.5, 80.0) should be(false) // Below coverage threshold
  }
  
  it should "calculate throughput correctly" in {
    calculateThroughput(1000, 1000) should be(1000.0) // 1000 items/second
    calculateThroughput(1000, 500) should be(2000.0) // 2000 items/second
    calculateThroughput(1000, 0) should be(0.0) // Handle zero time
  }
  
  it should "format percentages correctly" in {
    formatPercentage(95.6789, 2) should be("95.68%")
    formatPercentage(100.0, 1) should be("100.0%")
    formatPercentage(0.0, 0) should be("0%")
  }
  
  it should "format durations correctly" in {
    formatDuration(1000) should be("1s")
    formatDuration(60000) should be("1m 0s")
    formatDuration(3661000) should be("1h 1m")
    formatDuration(90061000) should be("1d 1h 1m") // 25 hours + 1 minute
  }
  
  it should "format memory sizes correctly" in {
    formatMemorySize(1024) should be("1.00 KB")
    formatMemorySize(1048576) should be("1.00 MB")
    formatMemorySize(1073741824) should be("1.00 GB")
    formatMemorySize(500) should be("500 B")
  }
  
  it should "format throughput correctly" in {
    formatThroughput(1500, "records") should be("1.50K records/sec")
    formatThroughput(2500000, "tracks") should be("2.50M tracks/sec")
    formatThroughput(50, "items") should be("50.00 items/sec")
  }
  
  it should "calculate average session length correctly" in {
    calculateAverageSessionLength(1000, 50) should be(20.0)
    calculateAverageSessionLength(0, 50) should be(0.0)
    calculateAverageSessionLength(1000, 0) should be(0.0) // Handle zero sessions
  }
  
  it should "calculate track ID coverage correctly" in {
    calculateTrackIdCoverage(900, 1000) should be(90.0)
    calculateTrackIdCoverage(0, 1000) should be(0.0)
    calculateTrackIdCoverage(1000, 0) should be(0.0) // Handle zero total
  }
  
  "PerformanceMonitor" should "track execution time" in {
    val result = timeExecution("test-operation") {
      Thread.sleep(50) // Simulate work
      "completed"
    }
    
    result should be("completed")
    
    val report = getPerformanceReport()
    report.metrics should not be empty
    
    val metric = report.metrics.find(_.operationName == "test-operation")
    metric should be(defined)
    metric.get.success should be(true)
    metric.get.duration.toMillis should be >= 50L
  }
  
  it should "track throughput correctly" in {
    trackThroughput("batch-test", 1000) {
      Thread.sleep(100) // Simulate processing 1000 items
      "processed"
    }
    
    val report = getPerformanceReport()
    val metric = report.metrics.find(_.operationName.contains("batch-test"))
    metric should be(defined)
    metric.get.success should be(true)
  }
  
  it should "handle failures correctly" in {
    assertThrows[RuntimeException] {
      timeExecution("failing-test") {
        throw new RuntimeException("Test failure")
      }
    }
    
    val report = getPerformanceReport()
    val metric = report.metrics.find(_.operationName == "failing-test")
    metric should be(defined)
    metric.get.success should be(false)
    metric.get.error should be(Some("Test failure"))
  }
  
  it should "generate performance reports" in {
    timeExecution("report-test") { "result" }
    
    val formattedReport = formatPerformanceReport()
    formattedReport should include("PERFORMANCE REPORT")
    formattedReport should include("Total Operations")
    formattedReport should include("report-test")
  }
  
  it should "clear metrics correctly" in {
    timeExecution("cleanup-test") { "result" }
    getPerformanceReport().metrics should not be empty
    
    clearMetrics()
    getPerformanceReport().metrics should be(empty)
  }
}