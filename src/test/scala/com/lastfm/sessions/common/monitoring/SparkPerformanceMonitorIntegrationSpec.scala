package com.lastfm.sessions.common.monitoring

import com.lastfm.sessions.pipelines.{DataCleaningPipeline, PipelineConfig, UserIdPartitionStrategy, QualityThresholds, SparkConfig}
import com.lastfm.sessions.utils.{ParquetTestSpec, BaseTestSpec}
import org.apache.spark.sql.DataFrame

/**
 * Integration tests for SparkPerformanceMonitor enhancement.
 * 
 * Tests minimal integration of Spark-specific monitoring capabilities
 * with existing pipeline infrastructure using surgical changes.
 * 
 * These tests ensure that:
 * 1. SparkPerformanceMonitor integration works correctly
 * 2. DataFrame analysis capabilities are available
 * 3. Existing functionality is preserved
 * 4. Performance monitoring provides meaningful insights
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SparkPerformanceMonitorIntegrationSpec extends ParquetTestSpec with BaseTestSpec {

  "DataCleaningPipeline with SparkPerformanceMonitor" should "provide Spark-specific monitoring capabilities" in {
    // Given: A pipeline with minimal test configuration  
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = createTempOutputDir("output"),
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 2, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new DataCleaningPipeline(config)

    // When: Checking for Spark-specific capabilities
    // Then: Should have Spark-specific insights (this tests the integration)
    pipeline.hasSparkMetrics should be(true)
    
    // And: Should still have existing performance monitoring capabilities
    val result = pipeline.timeExecution("test-operation") { "test-result" }
    result should be("test-result")
    
    // And: Should have performance metrics
    val performanceReport = pipeline.formatPerformanceReport()
    performanceReport should not be empty
  }

  it should "analyze DataFrame characteristics without changing core functionality" in {
    // Given: A simple DataFrame for analysis
    import spark.implicits._
    val testData = Seq(
      ("user1", "1234567890", "artist1", "Artist One", "track1", "Track One"),
      ("user2", "1234567891", "artist2", "Artist Two", "track2", "Track Two")
    ).toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName")

    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = createTempOutputDir("output"),
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 2, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new DataCleaningPipeline(config)

    // When: Analyzing DataFrame (tests the SparkPerformanceMonitor capability)
    val metrics = pipeline.analyzeDataFrame(testData, "test-data")

    // Then: Should provide comprehensive metrics
    metrics.name should be("test-data")
    metrics.records should be(2)
    metrics.columns should be(6)
    metrics.partitions should be > 0
  }

  it should "maintain existing performance monitoring while adding Spark insights" in {
    // Given: Pipeline configuration
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = createTempOutputDir("output"),
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 2, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new DataCleaningPipeline(config)

    // When: Executing with monitoring
    val result = pipeline.timeExecution("test-operation") {
      "test-result"
    }

    // Then: Should maintain existing functionality
    result should be("test-result")
    
    // And: Should have performance metrics
    val report = pipeline.formatPerformanceReport()
    report should include("test-operation")
  }

  it should "provide DataFrame analysis during actual pipeline execution" in {
    // Given: A realistic test scenario with actual data processing
    val testInput = createTsvTestFile(List(
      ("user1", "1234567890", "artist1", "Artist One", "track1", "Track One"),
      ("user2", "1234567891", "artist2", "Artist Two", "track2", "Track Two"),
      ("user3", "1234567892", "artist3", "Artist Three", "track3", "Track Three")
    ))
    val testOutput = createTempOutputDir("spark-integration-test")
    
    val config = PipelineConfig(
      bronzePath = testInput,
      silverPath = testOutput,
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 3, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new DataCleaningPipeline(config)

    // When: Executing the full pipeline (this should show DataFrame analysis in action)
    val result = pipeline.execute()

    // Then: Pipeline should complete successfully
    result.isSuccess should be(true)
    
    // And: Should have quality metrics
    result.get.qualityScore should be >= 90.0
    
    // And: Should have performance insights
    val performanceReport = pipeline.formatPerformanceReport()
    performanceReport should not be empty
  }

  /**
   * Helper method to create test TSV files.
   */
  private def createTsvTestFile(data: List[(String, String, String, String, String, String)]): String = {
    val testFile = s"${createTempOutputDir("test-input")}/test-data-${System.currentTimeMillis()}.tsv"
    val content = data.map { case (userId, timestamp, artistId, artistName, trackId, trackName) =>
      s"$userId\t$timestamp\t$artistId\t$artistName\t$trackId\t$trackName"
    }.mkString("\n")
    
    import java.nio.file.{Files, Paths}
    import java.nio.charset.StandardCharsets
    Files.write(Paths.get(testFile), content.getBytes(StandardCharsets.UTF_8))
    testFile
  }
}
