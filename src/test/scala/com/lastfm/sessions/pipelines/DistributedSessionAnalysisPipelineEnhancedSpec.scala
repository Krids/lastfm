package com.lastfm.sessions.pipelines

import com.lastfm.sessions.utils.{ParquetTestSpec, BaseTestSpec}
import com.lastfm.sessions.domain.DistributedSessionAnalysis
import org.apache.spark.sql.DataFrame

/**
 * Integration tests for DistributedSessionAnalysisPipeline with SparkPerformanceMonitor enhancement.
 * 
 * Tests the integration of Spark-specific monitoring capabilities with the session analysis
 * pipeline, ensuring enhanced DataFrame analysis and performance insights for Phase 2
 * of the data processing pipeline (Silver → Gold transformation).
 * 
 * Test Focus:
 * 1. SparkPerformanceMonitor integration functionality
 * 2. DataFrame analysis during session calculation
 * 3. Session processing performance insights
 * 4. Gold layer optimization monitoring
 * 5. Backward compatibility with existing functionality
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DistributedSessionAnalysisPipelineEnhancedSpec extends ParquetTestSpec with BaseTestSpec {

  "DistributedSessionAnalysisPipeline with SparkPerformanceMonitor" should "provide Spark-specific monitoring capabilities for session analysis" in {
    // Given: A session analysis pipeline with test configuration
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = createSessionTestDataFile(), // Silver layer with session data
      goldPath = createTempOutputDir("gold"),
      outputPath = createTempOutputDir("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 5, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new DistributedSessionAnalysisPipeline(config)

    // When: Checking for Spark-specific capabilities
    // Then: Should have enhanced monitoring capabilities (this will fail initially - TDD Red phase)
    pipeline.hasSparkMetrics should be(true)
    
    // And: Should have session-specific monitoring methods
    pipeline.canAnalyzeSessionDataFrames should be(true)
  }

  it should "analyze Silver DataFrame characteristics during session processing" in {
    // Given: A pipeline with Silver layer session data
    import spark.implicits._
    val sessionData = createSessionDataFrame()
    
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = createSessionTestDataFile(),
      goldPath = createTempOutputDir("gold"), 
      outputPath = createTempOutputDir("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 3, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new DistributedSessionAnalysisPipeline(config)

    // When: Analyzing session DataFrame characteristics
    val metrics = pipeline.analyzeDataFrame(sessionData, "session-input")

    // Then: Should provide comprehensive session-specific metrics
    metrics.name should be("session-input")
    metrics.records should be(6) // Based on our test data
    metrics.partitions should be > 0
    metrics.columns should be(6) // userId, timestamp, artistId, artistName, trackId, trackName
  }

  it should "provide session calculation performance insights during window operations" in {
    // Given: A realistic session analysis scenario with actual Silver data
    val silverPath = createSessionTestDataFile()
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = silverPath,
      goldPath = createTempOutputDir("gold"),
      outputPath = createTempOutputDir("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 3, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new DistributedSessionAnalysisPipeline(config)

    // When: Executing session analysis with monitoring
    val result = pipeline.monitorSessionCalculation("session-window-analysis") {
      // Simulate session calculation operation
      "session-calculation-complete"
    }

    // Then: Should capture session-specific performance data
    result should be("session-calculation-complete")
    
    // And: Should have performance metrics for session operations
    val performanceReport = pipeline.formatPerformanceReport()
    performanceReport should include("session-window-analysis")
  }

  it should "maintain existing session analysis functionality while adding monitoring" in {
    // Given: Standard session analysis configuration
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = createSessionTestDataFile(),
      goldPath = createTempOutputDir("gold"),
      outputPath = createTempOutputDir("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 3, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new DistributedSessionAnalysisPipeline(config)

    // When: Executing the full pipeline (this tests backward compatibility)
    val result = pipeline.execute()

    // Then: Should execute successfully with session analysis results
    result.isSuccess should be(true)
    
    // And: Should have DistributedSessionAnalysis with comprehensive metrics
    val analysis = result.get
    analysis.metrics.totalTracks should be > 0L
    analysis.metrics.totalSessions should be > 0L
    
    // And: Should have enhanced performance monitoring
    val performanceReport = pipeline.formatPerformanceReport()
    performanceReport should not be empty
  }

  /**
   * Helper method to create test session data file.
   */
  private def createSessionTestDataFile(): String = {
    val sessionData = List(
      // User 1: Two sessions with 20+ minute gap
      ("user1", "2009-05-04T10:00:00Z", "artist1", "Artist One", "track1", "Track One"),
      ("user1", "2009-05-04T10:05:00Z", "artist1", "Artist One", "track2", "Track Two"),
      ("user1", "2009-05-04T10:30:00Z", "artist2", "Artist Two", "track3", "Track Three"), // New session
      
      // User 2: Single session
      ("user2", "2009-05-04T11:00:00Z", "artist3", "Artist Three", "track4", "Track Four"),
      ("user2", "2009-05-04T11:10:00Z", "artist3", "Artist Three", "track5", "Track Five"),
      
      // User 3: Single event
      ("user3", "2009-05-04T12:00:00Z", "artist4", "Artist Four", "track6", "Track Six")
    )
    
    createTestParquetFile(sessionData, "session-test-data")
  }

  /**
   * Helper method to create session DataFrame for testing.
   */
  private def createSessionDataFrame(): DataFrame = {
    import spark.implicits._
    Seq(
      ("user1", "2009-05-04T10:00:00Z", "artist1", "Artist One", "track1", "Track One"),
      ("user1", "2009-05-04T10:05:00Z", "artist1", "Artist One", "track2", "Track Two"),
      ("user2", "2009-05-04T11:00:00Z", "artist3", "Artist Three", "track4", "Track Four"),
      ("user2", "2009-05-04T11:10:00Z", "artist3", "Artist Three", "track5", "Track Five"),
      ("user3", "2009-05-04T12:00:00Z", "artist4", "Artist Four", "track6", "Track Six"),
      ("user3", "2009-05-04T12:15:00Z", "artist4", "Artist Four", "track7", "Track Seven")
    ).toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName")
  }

  /**
   * Helper method to create test Parquet files for session analysis.
   * Includes trackKey field that session analysis pipeline expects.
   */
  private def createTestParquetFile(
    data: List[(String, String, String, String, String, String)], 
    name: String
  ): String = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    
    val df = data.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName")
    
    // Add trackKey field (same logic as DataCleaningPipeline)
    val enhancedDF = df.withColumn("trackKey",
      when($"trackId".isNotNull && $"trackId" =!= "", $"trackId")
      .otherwise(concat($"artistName", lit(" — "), $"trackName"))
    )
    
    val outputPath = s"${createTempOutputDir("session-test")}/$name.parquet"
    
    enhancedDF.write
      .mode("overwrite")
      .parquet(outputPath)
      
    outputPath
  }
}
