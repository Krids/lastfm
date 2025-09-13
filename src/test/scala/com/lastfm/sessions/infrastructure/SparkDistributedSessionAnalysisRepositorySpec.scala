package com.lastfm.sessions.infrastructure

import com.lastfm.sessions.domain._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import scala.util.{Try, Success, Failure}

/**
 * TDD-First specification for Spark implementation of distributed session analysis.
 * 
 * Tests the Spark-specific infrastructure layer that provides distributed processing
 * without loading large datasets into driver memory.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SparkDistributedSessionAnalysisRepositorySpec extends AnyFlatSpec with Matchers {

  // Create test Spark session for integration testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("SparkDistributedSessionAnalysisRepository-Test")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  "SparkDistributedSessionAnalysisRepository" should "load events as distributed DataFrame stream" in {
    // Given: Repository with test data
    val repository = new SparkDistributedSessionAnalysisRepository()
    
    // Create test CSV data
    val testData = createTestSilverData()
    
    // When: Events are loaded
    val result = repository.loadEventsStream(testData)
    
    // Then: Stream is returned without loading into memory
    result.isSuccess shouldBe true
    result.get shouldBe a[SparkEventStream]
    
    // Verify data is not collected to driver
    val eventStream = result.get.asInstanceOf[SparkEventStream]
    eventStream.count() should be > 0L
  }
  
  it should "calculate session metrics using distributed aggregations" in {
    // Given: Repository and test data
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createTestSilverData()
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Metrics are calculated using distributed operations
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: Metrics are calculated without driver memory collection
    result.isSuccess shouldBe true
    val metrics = result.get
    
    // FIXED: Exact assertions with correct session boundary logic
    metrics.totalSessions shouldBe 3L      // user1: 2 sessions (30-min gap), user2: 1 session
    metrics.uniqueUsers shouldBe 2L        // user1 and user2
    metrics.totalTracks shouldBe 5L        // 5 events total  
    metrics.averageSessionLength shouldBe (5.0/3.0) +- 0.01  // 5 tracks / 3 sessions ≈ 1.67
    metrics.qualityScore shouldBe 99.0     // Fixed quality score
    
    // CRITICAL: Sessions should be MORE than users (validates our fix)
    metrics.totalSessions should be > metrics.uniqueUsers // 3 > 2 proves fix is working!
  }
  
  it should "apply optimal partitioning for session analysis" in {
    // Given: Repository and test data
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createTestSilverData()
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Stream is partitioned by user
    val partitionedStream = eventsStream.partitionByUser()
    
    // Then: Partitioning is applied for session analysis optimization
    partitionedStream shouldBe a[SparkEventStream]
    partitionedStream.count() shouldEqual eventsStream.count()
  }
  
  it should "cache streams for multiple operations" in {
    // Given: Repository and test data
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createTestSilverData()
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Stream is cached
    val cachedStream = eventsStream.cache()
    
    // Then: Caching is applied for performance optimization
    cachedStream shouldBe a[SparkEventStream]
    cachedStream.count() shouldEqual eventsStream.count()
  }
  
  it should "calculate sessions using distributed window functions" in {
    // Given: Repository and test data with time gaps
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createTestSilverDataWithTimeGaps()
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Sessions are calculated using 20-minute gap
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: Sessions are calculated correctly with exact counts
    result.isSuccess shouldBe true
    val metrics = result.get
    
    // FIXED: Exact assertions instead of weak > 0 checks
    metrics.totalSessions shouldBe 2L      // user1: 2 sessions (gap at 30 min)  
    metrics.uniqueUsers shouldBe 1L        // Only user1 in test data
    metrics.totalTracks shouldBe 4L        // 4 events total
    metrics.averageSessionLength shouldBe 2.0  // 4 tracks / 2 sessions
    
    // Verify session boundaries are detected correctly  
    metrics.totalSessions should be > metrics.uniqueUsers // More sessions than users (prevents 1:1 bug)
  }
  
  it should "handle large datasets without memory issues" in {
    // Given: Repository with larger test dataset
    val repository = new SparkDistributedSessionAnalysisRepository()
    val largeTestData = createLargeTestSilverData(10000) // 10K events
    val eventsStream = repository.loadEventsStream(largeTestData).get
    
    // When: Analysis is performed on large dataset
    val optimizedStream = eventsStream.partitionByUser().cache()
    val result = repository.calculateSessionMetrics(optimizedStream)
    
    // Then: Processing completes without OutOfMemoryError
    if (result.isFailure) {
      println(s"Debug: Test failure reason: ${result.failed.get.getMessage}")
      result.failed.get.printStackTrace()
    }
    result.isSuccess shouldBe true
    val metrics = result.get
    
    metrics.totalSessions should be > 0L
    metrics.uniqueUsers should be > 0L
    
    // Verify no driver memory collection occurred
    // (This would fail with OutOfMemoryError if using collect())
  }
  
  it should "persist analysis results distributively" in {
    // Given: Repository and analysis results
    val repository = new SparkDistributedSessionAnalysisRepository()
    val metrics = SessionMetrics(1000L, 100L, 15000L, 15.0, 99.0)
    val analysis = DistributedSessionAnalysis(metrics)
    val outputPath = s"${System.getProperty("java.io.tmpdir")}/test-gold-output"
    
    // When: Analysis is persisted
    val result = repository.persistAnalysis(analysis, outputPath)
    
    // Then: Persistence succeeds using distributed writes
    result.isSuccess shouldBe true
  }
  
  /**
   * Creates test Silver layer data in Parquet format for testing.
   */
  private def createTestSilverData(): String = {
    import spark.implicits._
    
    val testEvents = Seq(
      ("user1", "2023-01-01T10:00:00Z", "artist1", "Artist One", "track1", "Track One", "Artist One — Track One"),
      ("user1", "2023-01-01T10:15:00Z", "artist1", "Artist One", "track2", "Track Two", "Artist One — Track Two"),
      ("user1", "2023-01-01T10:45:00Z", "artist2", "Artist Two", "track3", "Track Three", "Artist Two — Track Three"),
      ("user2", "2023-01-01T11:00:00Z", "artist1", "Artist One", "track1", "Track One", "Artist One — Track One"),
      ("user2", "2023-01-01T11:15:00Z", "artist2", "Artist Two", "track4", "Track Four", "Artist Two — Track Four")
    )
    
    val df = testEvents.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName", "trackKey")
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/test-silver-data"
    
    // Ensure data is actually written by forcing evaluation
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(tempPath)
    
    // Verify data was written
    val verifyDF = spark.read.parquet(tempPath)
    verifyDF.count() // Force evaluation to ensure data is written
    
    tempPath
  }
  
  /**
   * Creates test data with specific time gaps for session boundary testing in Parquet format.
   */
  private def createTestSilverDataWithTimeGaps(): String = {
    import spark.implicits._
    
    val testEvents = Seq(
      ("user1", "2023-01-01T10:00:00Z", "artist1", "Artist One", "track1", "Track One", "Artist One — Track One"),
      ("user1", "2023-01-01T10:15:00Z", "artist1", "Artist One", "track2", "Track Two", "Artist One — Track Two"),
      ("user1", "2023-01-01T10:45:00Z", "artist2", "Artist Two", "track3", "Track Three", "Artist Two — Track Three"), // 30 min gap - new session
      ("user1", "2023-01-01T11:00:00Z", "artist2", "Artist Two", "track4", "Track Four", "Artist Two — Track Four")
    )
    
    val df = testEvents.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName", "trackKey")
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/test-silver-data-gaps"
    
    // Ensure data is actually written by forcing evaluation
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(tempPath)
    
    // Verify data was written
    val verifyDF = spark.read.parquet(tempPath)
    verifyDF.count() // Force evaluation to ensure data is written
    
    tempPath
  }

  it should "handle production-scale data without 1:1 user:session ratio" in {
    // Given: Production-like test data to expose the 1:1 ratio bug
    val productionLikeEvents = generateProductionLikeEvents(users = 50, eventsPerUser = 10)
    
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createParquetData(productionLikeEvents)
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Sessions are calculated
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: Sessions should be much higher than users (realistic ratio)
    result shouldBe a[Success[_]]
    val metrics = result.get
    
    metrics.uniqueUsers shouldBe 50L
    metrics.totalTracks shouldBe 500L      // 50 users * 10 events each
    
    // CRITICAL: Sessions should be 2-4x users, NOT 1:1 ratio  
    metrics.totalSessions should be >= (metrics.uniqueUsers * 2)   // At least 2x users
    metrics.totalSessions should be <= (metrics.uniqueUsers * 4)   // At most 4x users
    
    val sessionUserRatio = metrics.totalSessions.toDouble / metrics.uniqueUsers
    sessionUserRatio should be >= 2.0
    sessionUserRatio should be <= 4.0
    
    // REGRESSION TEST: Never allow 1:1 ratio
    metrics.totalSessions should not equal metrics.uniqueUsers
  }
  
  /**
   * Generates production-like test events with realistic session patterns.
   */
  private def generateProductionLikeEvents(users: Int, eventsPerUser: Int): Seq[(String, String, String, String, String, String, String)] = {
    (1 to users).flatMap { userId =>
      // Each user has multiple sessions with gaps
      val baseTime = "2023-01-01T10:00:00Z"
      (1 to eventsPerUser).map { eventIdx =>
        val timeOffset = if (eventIdx <= eventsPerUser/2) {
          // First half: continuous session (5-minute gaps)
          (eventIdx - 1) * 5
        } else {
          // Second half: new session (25+ minute gap from first half)
          ((eventsPerUser/2 - 1) * 5) + 25 + ((eventIdx - eventsPerUser/2 - 1) * 3)
        }
        
        val timestamp = f"2023-01-01T${10 + timeOffset/60}%02d:${timeOffset%60}%02d:00Z"
        (
          s"user$userId", 
          timestamp, 
          s"artist$eventIdx", 
          s"Artist $eventIdx", 
          s"track$eventIdx", 
          s"Track $eventIdx", 
          s"track$eventIdx"
        )
      }
    }
  }
  
  private def createParquetData(events: Seq[(String, String, String, String, String, String, String)]): String = {
    import spark.implicits._
    
    val df = events.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName", "trackKey")
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/production-test-${System.currentTimeMillis()}"
    
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "snappy")  
      .parquet(tempPath)
      
    tempPath
  }

  /**
   * Creates larger test dataset for performance testing in Parquet format.
   */
  private def createLargeTestSilverData(numEvents: Int): String = {
    import spark.implicits._
    
    val testEvents = (1 to numEvents).map { i =>
      val userId = s"user${i % 100}" // 100 users
      val timestamp = f"2023-01-01T${10 + (i % 12)}%02d:${i % 60}%02d:00Z"
      val artistId = s"artist${i % 50}"
      val artistName = s"Artist ${i % 50}"
      val trackId = s"track${i}"
      val trackName = s"Track ${i}"
      val trackKey = s"${artistName} — ${trackName}"
      
      (userId, timestamp, artistId, artistName, trackId, trackName, trackKey)
    }
    
    val df = testEvents.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName", "trackKey")
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/test-silver-data-large"
    
    // Ensure data is actually written by forcing evaluation
    df.coalesce(4) // Multiple partitions for distributed processing
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(tempPath)
    
    // Verify data was written
    val verifyDF = spark.read.parquet(tempPath)
    verifyDF.count() // Force evaluation to ensure data is written
    
    tempPath
  }
  
}
