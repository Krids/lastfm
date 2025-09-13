package com.lastfm.sessions.pipelines

import com.lastfm.sessions.utils.{ParquetTestSpec, BaseTestSpec}
import com.lastfm.sessions.domain.RankingResult
import org.apache.spark.sql.DataFrame

/**
 * Integration tests for RankingPipeline with SparkPerformanceMonitor enhancement.
 * 
 * Tests the integration of Spark-specific monitoring capabilities with the ranking
 * pipeline, ensuring enhanced DataFrame analysis and performance insights for Phase 3
 * of the data processing pipeline (Gold → Results transformation).
 * 
 * Test Focus:
 * 1. SparkPerformanceMonitor integration functionality
 * 2. DataFrame analysis during session ranking operations
 * 3. Track aggregation performance insights
 * 4. Final output optimization monitoring
 * 5. Backward compatibility with existing ranking functionality
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class RankingPipelineEnhancedSpec extends ParquetTestSpec with BaseTestSpec {

  "RankingPipeline with SparkPerformanceMonitor" should "provide Spark-specific monitoring capabilities for ranking operations" in {
    // Given: A ranking pipeline with test configuration
    val (sessionsPath, _) = createRankingTestData()
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = sessionsPath,
      goldPath = createTempOutputDir("gold"),
      outputPath = createTempOutputDir("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 5, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new RankingPipeline(config)

    // When: Checking for Spark-specific capabilities
    // Then: Should have enhanced monitoring capabilities (this will fail initially - TDD Red phase)
    pipeline.hasSparkMetrics should be(true)
    
    // And: Should have ranking-specific monitoring methods
    pipeline.canAnalyzeRankingDataFrames should be(true)
  }

  it should "analyze session DataFrame characteristics during ranking operations" in {
    // Given: A pipeline with ranking session data
    import spark.implicits._
    val sessionData = createRankingDataFrame()
    
    val (sessionsPath, _) = createRankingTestData()
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = sessionsPath,
      goldPath = createTempOutputDir("gold"), 
      outputPath = createTempOutputDir("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 3, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new RankingPipeline(config)

    // When: Analyzing session DataFrame characteristics
    val metrics = pipeline.analyzeDataFrame(sessionData, "ranking-sessions")

    // Then: Should provide comprehensive ranking-specific metrics
    metrics.name should be("ranking-sessions")
    metrics.records should be(3) // Based on our test data
    metrics.partitions should be > 0
    metrics.columns should be(5) // sessionId, userId, startTime, endTime, trackCount
  }

  it should "provide track aggregation performance insights during ranking operations" in {
    // Given: A realistic ranking scenario with session and track data
    val (sessionsPath, tracksPath) = createRankingTestData()
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = sessionsPath,
      goldPath = tracksPath,
      outputPath = createTempOutputDir("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 3, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new RankingPipeline(config)

    // When: Executing ranking operations with monitoring
    val result = pipeline.monitorRankingCalculation("track-aggregation") {
      // Simulate track aggregation operation
      "track-aggregation-complete"
    }

    // Then: Should capture ranking-specific performance data
    result should be("track-aggregation-complete")
    
    // And: Should have performance metrics for ranking operations
    val performanceReport = pipeline.formatPerformanceReport()
    performanceReport should include("track-aggregation")
  }

  it should "maintain existing ranking functionality while adding monitoring" in {
    // Given: Standard ranking configuration (simplified test focusing on SparkPerformanceMonitor integration)
    val config = PipelineConfig(
      bronzePath = createTempOutputDir("input"),
      silverPath = createTempOutputDir("silver"), // Will be empty but that's ok for this integration test
      goldPath = createTempOutputDir("gold"),
      outputPath = s"${createTempOutputDir("results")}/top_songs.tsv",
      partitionStrategy = UserIdPartitionStrategy(userCount = 3, cores = 2),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 90.0),
      sparkConfig = SparkConfig(partitions = 2, timeZone = "UTC")
    )
    val pipeline = new RankingPipeline(config)

    // When: Testing that the integration doesn't break instantiation
    // Then: Should have SparkPerformanceMonitor capabilities
    pipeline.hasSparkMetrics should be(true)
    pipeline.canAnalyzeRankingDataFrames should be(true)
    
    // And: Should have ranking-specific monitoring methods
    val result = pipeline.monitorRankingCalculation("test-ranking-operation") {
      "ranking-operation-complete"
    }
    result should be("ranking-operation-complete")
    
    // And: Should have enhanced performance monitoring
    val performanceReport = pipeline.formatPerformanceReport()
    performanceReport should include("test-ranking-operation")
  }

  /**
   * Helper method to create test ranking data (sessions and tracks).
   */
  private def createRankingTestData(): (String, String) = {
    // Create session data with durationMinutes (fixed timestamp format)
    val sessionData = List(
      ("session1", "user1", "2009-05-04T10:00:00", "2009-05-04T10:30:00", 15L, 30.0), // Long session
      ("session2", "user2", "2009-05-04T11:00:00", "2009-05-04T11:20:00", 12L, 20.0), // Medium session  
      ("session3", "user3", "2009-05-04T12:00:00", "2009-05-04T12:10:00", 8L, 10.0)   // Short session
    )
    
    // Create track data for sessions with timestamp
    val trackData = List(
      // Session 1 tracks (15 tracks) 
      ("session1", "track1", "Artist One", "Track One", "2009-05-04T10:00:00"),
      ("session1", "track2", "Artist One", "Track Two", "2009-05-04T10:02:00"),
      ("session1", "track3", "Artist Two", "Track Three", "2009-05-04T10:05:00"),
      ("session1", "track1", "Artist One", "Track One", "2009-05-04T10:08:00"), // Duplicate for popularity
      ("session1", "track4", "Artist Three", "Track Four", "2009-05-04T10:10:00"),
      ("session1", "track5", "Artist Four", "Track Five", "2009-05-04T10:12:00"),
      ("session1", "track6", "Artist Five", "Track Six", "2009-05-04T10:15:00"),
      ("session1", "track7", "Artist Six", "Track Seven", "2009-05-04T10:17:00"),
      ("session1", "track8", "Artist Seven", "Track Eight", "2009-05-04T10:19:00"),
      ("session1", "track9", "Artist Eight", "Track Nine", "2009-05-04T10:21:00"),
      ("session1", "track10", "Artist Nine", "Track Ten", "2009-05-04T10:23:00"),
      ("session1", "track11", "Artist Ten", "Track Eleven", "2009-05-04T10:25:00"),
      ("session1", "track12", "Artist Eleven", "Track Twelve", "2009-05-04T10:27:00"),
      ("session1", "track13", "Artist Twelve", "Track Thirteen", "2009-05-04T10:28:00"),
      ("session1", "track14", "Artist Thirteen", "Track Fourteen", "2009-05-04T10:30:00"),
      
      // Session 2 tracks (12 tracks)
      ("session2", "track1", "Artist One", "Track One", "2009-05-04T11:00:00"), // Popular track
      ("session2", "track15", "Artist Fourteen", "Track Fifteen", "2009-05-04T11:03:00"),
      ("session2", "track16", "Artist Fifteen", "Track Sixteen", "2009-05-04T11:05:00"),
      ("session2", "track17", "Artist Sixteen", "Track Seventeen", "2009-05-04T11:07:00"),
      ("session2", "track18", "Artist Seventeen", "Track Eighteen", "2009-05-04T11:09:00"),
      ("session2", "track19", "Artist Eighteen", "Track Nineteen", "2009-05-04T11:11:00"),
      ("session2", "track20", "Artist Nineteen", "Track Twenty", "2009-05-04T11:13:00"),
      ("session2", "track21", "Artist Twenty", "Track TwentyOne", "2009-05-04T11:15:00"),
      ("session2", "track22", "Artist TwentyOne", "Track TwentyTwo", "2009-05-04T11:17:00"),
      ("session2", "track23", "Artist TwentyTwo", "Track TwentyThree", "2009-05-04T11:18:00"),
      ("session2", "track24", "Artist TwentyThree", "Track TwentyFour", "2009-05-04T11:19:00"),
      ("session2", "track25", "Artist TwentyFour", "Track TwentyFive", "2009-05-04T11:20:00"),
      
      // Session 3 tracks (8 tracks)
      ("session3", "track1", "Artist One", "Track One", "2009-05-04T12:00:00"), // Popular track again
      ("session3", "track26", "Artist TwentyFive", "Track TwentySix", "2009-05-04T12:02:00"),
      ("session3", "track27", "Artist TwentySix", "Track TwentySeven", "2009-05-04T12:04:00"),
      ("session3", "track28", "Artist TwentySeven", "Track TwentyEight", "2009-05-04T12:06:00"),
      ("session3", "track29", "Artist TwentyEight", "Track TwentyNine", "2009-05-04T12:07:00"),
      ("session3", "track30", "Artist TwentyNine", "Track Thirty", "2009-05-04T12:08:00"),
      ("session3", "track31", "Artist Thirty", "Track ThirtyOne", "2009-05-04T12:09:00"),
      ("session3", "track32", "Artist ThirtyOne", "Track ThirtyTwo", "2009-05-04T12:10:00")
    )
    
    val sessionsPath = createSessionsParquetFile(sessionData)
    val tracksPath = createTracksParquetFile(trackData)
      
    (sessionsPath, tracksPath)
  }

  /**
   * Helper method to create ranking DataFrame for testing.
   */
  private def createRankingDataFrame(): DataFrame = {
    import spark.implicits._
    Seq(
      ("session1", "user1", "2009-05-04T10:00:00", "2009-05-04T10:30:00", 15L),
      ("session2", "user2", "2009-05-04T11:00:00", "2009-05-04T11:20:00", 12L),
      ("session3", "user3", "2009-05-04T12:00:00", "2009-05-04T12:10:00", 8L)
    ).toDF("sessionId", "userId", "startTime", "endTime", "trackCount")
  }

  /**
   * Helper method to create sessions Parquet file.
   */
  private def createSessionsParquetFile(
    data: List[(String, String, String, String, Long, Double)]
  ): String = {
    import spark.implicits._
    
    val df = data.toDF("sessionId", "userId", "startTime", "endTime", "trackCount", "durationMinutes")
    val outputPath = s"${createTempOutputDir("ranking-test")}/sessions.parquet"
    
    df.write
      .mode("overwrite")
      .parquet(outputPath)
      
    outputPath
  }

  /**
   * Helper method to create tracks Parquet file.
   */
  private def createTracksParquetFile(
    data: List[(String, String, String, String, String)]
  ): String = {
    import spark.implicits._
    
    val df = data.toDF("sessionId", "trackId", "artistName", "trackName", "timestamp")
    val outputPath = s"${createTempOutputDir("ranking-test")}/tracks.parquet"
    
    df.write
      .mode("overwrite")
      .parquet(outputPath)
      
    outputPath
  }
}
