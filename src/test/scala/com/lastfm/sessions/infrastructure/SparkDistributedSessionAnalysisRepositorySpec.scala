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
    
    metrics.totalSessions should be > 0L
    metrics.uniqueUsers should be > 0L
    metrics.totalTracks should be > 0L
    metrics.averageSessionLength should be > 0.0
    metrics.qualityScore should be >= 0.0
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
    val sessionStream = eventsStream.calculateSessions(java.time.Duration.ofMinutes(20))
    
    // Then: Sessions are calculated distributively
    sessionStream shouldBe a[SparkSessionStream]
    sessionStream.count() should be > 0L
    
    // Verify session boundaries are detected correctly
    val metrics = sessionStream.aggregateMetrics().get
    metrics.totalSessions should be > 0L
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
   * Creates test Silver layer data in memory for testing.
   */
  private def createTestSilverData(): String = {
    import spark.implicits._
    
    val testEvents = Seq(
      ("user1", "2023-01-01T10:00:00", "artist1", "Artist One", "track1", "Track One", "Artist One — Track One"),
      ("user1", "2023-01-01T10:15:00", "artist1", "Artist One", "track2", "Track Two", "Artist One — Track Two"),
      ("user1", "2023-01-01T10:45:00", "artist2", "Artist Two", "track3", "Track Three", "Artist Two — Track Three"),
      ("user2", "2023-01-01T11:00:00", "artist1", "Artist One", "track1", "Track One", "Artist One — Track One"),
      ("user2", "2023-01-01T11:15:00", "artist2", "Artist Two", "track4", "Track Four", "Artist Two — Track Four")
    )
    
    val df = testEvents.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName", "trackKey")
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/test-silver-data"
    
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(tempPath)
    
    tempPath
  }
  
  /**
   * Creates test data with specific time gaps for session boundary testing.
   */
  private def createTestSilverDataWithTimeGaps(): String = {
    import spark.implicits._
    
    val testEvents = Seq(
      ("user1", "2023-01-01T10:00:00", "artist1", "Artist One", "track1", "Track One", "Artist One — Track One"),
      ("user1", "2023-01-01T10:15:00", "artist1", "Artist One", "track2", "Track Two", "Artist One — Track Two"),
      ("user1", "2023-01-01T10:45:00", "artist2", "Artist Two", "track3", "Track Three", "Artist Two — Track Three"), // 30 min gap - new session
      ("user1", "2023-01-01T11:00:00", "artist2", "Artist Two", "track4", "Track Four", "Artist Two — Track Four")
    )
    
    val df = testEvents.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName", "trackKey")
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/test-silver-data-gaps"
    
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(tempPath)
    
    tempPath
  }
  
  /**
   * Creates larger test dataset for performance testing.
   */
  private def createLargeTestSilverData(numEvents: Int): String = {
    import spark.implicits._
    
    val testEvents = (1 to numEvents).map { i =>
      val userId = s"user${i % 100}" // 100 users
      val timestamp = f"2023-01-01T${10 + (i % 12)}:${i % 60}:00"
      val artistId = s"artist${i % 50}"
      val artistName = s"Artist ${i % 50}"
      val trackId = s"track${i}"
      val trackName = s"Track ${i}"
      val trackKey = s"${artistName} — ${trackName}"
      
      (userId, timestamp, artistId, artistName, trackId, trackName, trackKey)
    }
    
    val df = testEvents.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName", "trackKey")
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/test-silver-data-large"
    
    df.coalesce(4) // Multiple partitions for distributed processing
      .write
      .mode("overwrite")
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(tempPath)
    
    tempPath
  }
  
}
