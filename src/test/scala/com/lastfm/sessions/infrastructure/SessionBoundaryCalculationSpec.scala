package com.lastfm.sessions.infrastructure

import com.lastfm.sessions.domain._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Success

/**
 * Comprehensive tests for session boundary calculation logic.
 * 
 * CRITICAL: These tests prevent the regression where sessions = users (1:1 ratio).
 * Each test validates specific session boundary scenarios with exact assertions.
 * 
 * The previous bug: First events didn't start sessions, causing users without
 * 20+ minute gaps to have 0 sessions, leading to totalSessions = uniqueUsers.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SessionBoundaryCalculationSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("SessionBoundaryCalculation-Test")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  "Session boundary calculation" should "always start a new session for first user event" in {
    // Given: Single event for a user (this was the core bug - no session created)
    val testEvents = Seq(
      ("user1", "2023-01-01T10:00:00Z", "artist1", "Artist One", "track1", "Track One", "track1")
    )
    
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createParquetData(testEvents)
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Sessions are calculated  
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: First event MUST start exactly 1 session
    result shouldBe a[Success[_]]
    val metrics = result.get
    metrics.totalSessions shouldBe 1L      // ❌ FAILED with bug (was 0)
    metrics.uniqueUsers shouldBe 1L
    metrics.totalTracks shouldBe 1L
  }
  
  it should "create multiple sessions when gaps exceed 20 minutes" in {
    // Given: Events with 30-minute gap (exceeds 20-minute threshold)
    val testEvents = Seq(
      ("user1", "2023-01-01T10:00:00Z", "artist1", "Artist One", "track1", "Track One", "track1"),
      ("user1", "2023-01-01T10:15:00Z", "artist1", "Artist One", "track2", "Track Two", "track2"), // Same session
      ("user1", "2023-01-01T10:45:00Z", "artist2", "Artist Two", "track3", "Track Three", "track3"), // NEW SESSION (30 min gap)
      ("user1", "2023-01-01T11:00:00Z", "artist2", "Artist Two", "track4", "Track Four", "track4")  // Same session
    )
    
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createParquetData(testEvents)
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Sessions are calculated
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: Exactly 2 sessions should be created
    result shouldBe a[Success[_]]
    val metrics = result.get
    metrics.totalSessions shouldBe 2L      // ❌ FAILED with bug (was 1)
    metrics.uniqueUsers shouldBe 1L
    metrics.totalTracks shouldBe 4L
    metrics.averageSessionLength shouldBe 2.0  // 4 tracks / 2 sessions
  }
  
  it should "handle multiple users with different session patterns" in {
    // Given: Multiple users with different listening patterns
    val testEvents = Seq(
      // User 1: 2 sessions (gap at 30 minutes)
      ("user1", "2023-01-01T10:00:00Z", "artist1", "Artist One", "track1", "Track One", "track1"),
      ("user1", "2023-01-01T10:15:00Z", "artist1", "Artist One", "track2", "Track Two", "track2"),
      ("user1", "2023-01-01T10:45:00Z", "artist2", "Artist Two", "track3", "Track Three", "track3"), // New session
      
      // User 2: 1 session (all within 20 minutes)
      ("user2", "2023-01-01T11:00:00Z", "artist3", "Artist Three", "track4", "Track Four", "track4"),
      ("user2", "2023-01-01T11:10:00Z", "artist3", "Artist Three", "track5", "Track Five", "track5"),
      
      // User 3: 3 sessions (each event 25+ minutes apart)
      ("user3", "2023-01-01T12:00:00Z", "artist4", "Artist Four", "track6", "Track Six", "track6"),
      ("user3", "2023-01-01T12:30:00Z", "artist4", "Artist Four", "track7", "Track Seven", "track7"), // New session
      ("user3", "2023-01-01T13:00:00Z", "artist4", "Artist Four", "track8", "Track Eight", "track8")  // New session
    )
    
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createParquetData(testEvents)
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Sessions are calculated
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: Sessions should be 2 + 1 + 3 = 6 total
    result shouldBe a[Success[_]]
    val metrics = result.get
    metrics.totalSessions shouldBe 6L     // ❌ FAILED with bug (was 3 = users)
    metrics.uniqueUsers shouldBe 3L
    metrics.totalTracks shouldBe 8L
    
    // CRITICAL: Sessions should NOT equal users
    metrics.totalSessions should not equal metrics.uniqueUsers
  }
  
  it should "never have sessions equal to users (regression test)" in {
    // Given: Data that would expose the 1:1 user:session bug
    val testEvents = (1 to 20).flatMap { userId =>
      Seq(
        (s"user$userId", "2023-01-01T10:00:00Z", "artist1", "Artist One", "track1", "Track One", "track1"),
        (s"user$userId", "2023-01-01T10:10:00Z", "artist1", "Artist One", "track2", "Track Two", "track2"),
        (s"user$userId", "2023-01-01T10:35:00Z", "artist2", "Artist Two", "track3", "Track Three", "track3"), // New session  
        (s"user$userId", "2023-01-01T10:45:00Z", "artist2", "Artist Two", "track4", "Track Four", "track4")
      )
    }
    
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createParquetData(testEvents)
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Sessions are calculated
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: Sessions should be 2x users, NOT equal to users
    result shouldBe a[Success[_]]
    val metrics = result.get
    metrics.totalSessions shouldBe 40L    // 20 users * 2 sessions each
    metrics.uniqueUsers shouldBe 20L      
    metrics.totalTracks shouldBe 80L      // 20 users * 4 tracks each
    
    // CRITICAL REGRESSION TEST:
    metrics.totalSessions should not equal metrics.uniqueUsers  // ❌ FAILED with bug
    (metrics.totalSessions.toDouble / metrics.uniqueUsers) shouldBe 2.0 +- 0.1
  }
  
  it should "handle continuous listening within session threshold" in {
    // Given: User with continuous listening (no gaps > 20 minutes)
    val testEvents = Seq(
      ("user1", "2023-01-01T10:00:00Z", "artist1", "Artist One", "track1", "Track One", "track1"),
      ("user1", "2023-01-01T10:05:00Z", "artist1", "Artist One", "track2", "Track Two", "track2"),  // 5 min gap
      ("user1", "2023-01-01T10:15:00Z", "artist2", "Artist Two", "track3", "Track Three", "track3"), // 10 min gap  
      ("user1", "2023-01-01T10:30:00Z", "artist2", "Artist Two", "track4", "Track Four", "track4"),  // 15 min gap
      ("user1", "2023-01-01T10:48:00Z", "artist3", "Artist Three", "track5", "Track Five", "track5") // 18 min gap
    )
    
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createParquetData(testEvents)
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Sessions are calculated
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: All events should be in exactly 1 session
    result shouldBe a[Success[_]]
    val metrics = result.get
    metrics.totalSessions shouldBe 1L      // ❌ FAILED with bug (was 0)
    metrics.uniqueUsers shouldBe 1L
    metrics.totalTracks shouldBe 5L
    metrics.averageSessionLength shouldBe 5.0
  }
  
  it should "create session for every event when all gaps exceed threshold" in {
    // Given: Events with gaps all exceeding 20 minutes
    val testEvents = Seq(
      ("user1", "2023-01-01T10:00:00Z", "artist1", "Artist One", "track1", "Track One", "track1"),
      ("user1", "2023-01-01T10:30:00Z", "artist1", "Artist One", "track2", "Track Two", "track2"), // 30 min gap
      ("user1", "2023-01-01T11:00:00Z", "artist2", "Artist Two", "track3", "Track Three", "track3"), // 30 min gap
      ("user1", "2023-01-01T11:35:00Z", "artist2", "Artist Two", "track4", "Track Four", "track4")  // 35 min gap
    )
    
    val repository = new SparkDistributedSessionAnalysisRepository()
    val testData = createParquetData(testEvents)
    val eventsStream = repository.loadEventsStream(testData).get
    
    // When: Sessions are calculated
    val result = repository.calculateSessionMetrics(eventsStream)
    
    // Then: Each event should start a new session
    result shouldBe a[Success[_]]
    val metrics = result.get
    metrics.totalSessions shouldBe 4L      // Each event = new session
    metrics.uniqueUsers shouldBe 1L
    metrics.totalTracks shouldBe 4L
    metrics.averageSessionLength shouldBe 1.0  // 1 track per session
  }
  
  it should "generate many sessions from production data (not just one per user)" in {
    // This test checks actual production data to ensure we get ~1M sessions, not just 992
    import spark.implicits._
    import java.nio.file.Files
    import java.nio.file.Paths
    
    val silverPath = "data/output/silver/listening-events-cleaned.parquet"
    
    // Only run if production data exists
    if (Files.exists(Paths.get(silverPath))) {
      val repository = new SparkDistributedSessionAnalysisRepository()
      
      // Test with first 10 users to verify session calculation
      val sampleDF = spark.read.parquet(silverPath)
        .filter($"userId".isin("user_000001", "user_000002", "user_000003", "user_000004", "user_000005",
                               "user_000006", "user_000007", "user_000008", "user_000009", "user_000010"))
      
      println(s"\n=== Testing with 10 users, ${sampleDF.count()} events ===")
      
      // Debug: Check timestamp format and gaps
      println("\n=== Sample data with time gaps ===")
      import org.apache.spark.sql.expressions.Window
      val userWindow = Window.partitionBy("userId").orderBy("timestamp")
      
      val withGaps = sampleDF
        .withColumn("timestamp_parsed", to_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("prevTimestamp", lag("timestamp_parsed", 1).over(userWindow))
        .withColumn("timeGapSeconds", 
          when($"prevTimestamp".isNull, 0L)
          .otherwise(($"timestamp_parsed".cast("long") - $"prevTimestamp".cast("long"))))
        .filter($"timeGapSeconds" > 1200) // Show gaps > 20 minutes
        
      withGaps.select("userId", "timestamp", "timestamp_parsed", "prevTimestamp", "timeGapSeconds")
        .show(20, truncate = false)
      
      val gapsOver20Min = withGaps.count()
      println(s"Events with gaps > 20 minutes: $gapsOver20Min")
      
      // Calculate sessions
      val eventsStream = repository.loadEventsStream(silverPath).get
      val result = repository.calculateSessionMetrics(eventsStream)
      
      result shouldBe a[Success[_]]
      val metrics = result.get
      
      println(s"\n=== Session Metrics ===")
      println(s"Total Sessions: ${metrics.totalSessions}")
      println(s"Unique Users: ${metrics.uniqueUsers}")
      println(s"Sessions per User: ${metrics.totalSessions.toDouble / metrics.uniqueUsers}")
      
      // CRITICAL ASSERTION: We should have many more sessions than users
      // Based on gaps in the data, we expect at least 100 sessions per user on average
      metrics.totalSessions should be > (metrics.uniqueUsers * 10)
      
      // The bug would cause sessions = users (992 sessions for 992 users)
      metrics.totalSessions should not equal metrics.uniqueUsers
    } else {
      pending // Skip if production data doesn't exist
    }
  }
  
  private def createParquetData(events: Seq[(String, String, String, String, String, String, String)]): String = {
    import spark.implicits._
    
    val df = events.toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName", "trackKey")
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/session-boundary-test-${System.currentTimeMillis()}"
    
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "snappy")  
      .parquet(tempPath)
      
    tempPath
  }
}