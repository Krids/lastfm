package com.lastfm.sessions.infrastructure

import com.lastfm.sessions.domain._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.time.Duration
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

/**
 * Complete session dataset generator following data engineering best practices.
 * 
 * Generates comprehensive session datasets in Parquet format with optimal partitioning
 * for session analysis workloads. Implements single responsibility principle by
 * focusing solely on session dataset generation and persistence.
 * 
 * Key Features:
 * - Memory-efficient distributed processing (no driver-side data collection)
 * - Strategic userId partitioning for optimal session queries
 * - Parquet format with compression for storage efficiency
 * - Comprehensive session metadata generation
 * - Referential integrity across related datasets
 * 
 * Output Datasets:
 * - Session Details: Complete session information with metadata
 * - Session-Track Relationships: Track-level session associations
 * - Session Analytics: Business insights and patterns
 * - Top Sessions: Ranking of longest sessions
 * 
 * @param spark Implicit Spark session for distributed processing
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class CompleteSessionDatasetGenerator(implicit spark: SparkSession) {
  
  import spark.implicits._
  
  /**
   * Persists complete session details as userId-partitioned Parquet files.
   * 
   * Generates comprehensive session dataset including:
   * - Session metadata (ID, user, start/end times, duration)
   * - Session statistics (track count, unique tracks, duration)
   * - Session quality metrics (completeness, consistency)
   * 
   * Storage Strategy:
   * - Parquet format with Snappy compression for efficiency
   * - Partitioned by userId for optimal session analysis queries
   * - Optimized schema for columnar query performance
   * 
   * @param eventsStream Distributed stream of listening events
   * @param outputPath Path for session details output
   * @return Try indicating success or failure
   */
  def persistSessionDetails(eventsStream: EventStream, outputPath: String): Try[Unit] = {
    require(eventsStream != null, "eventsStream cannot be null")
    require(outputPath != null && outputPath.nonEmpty, "outputPath cannot be null or empty")
    
    try {
      println(s"💾 Generating session details dataset: $outputPath")
      
      // Calculate sessions using distributed window functions
      val sparkStream = eventsStream.asInstanceOf[SparkEventStream]
      val sessionsDF = calculateSessionsWithMetadata(sparkStream.df)
      
      // Write as userId-partitioned Parquet
      sessionsDF
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("userId")
        .parquet(outputPath)
      
      val sessionCount = sessionsDF.count()
      println(s"✅ Session details persisted: $sessionCount sessions")
      
      Success(())
      
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Failed to persist session details: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Persists session-track relationships with dual partitioning.
   * 
   * Generates detailed session-track mapping including:
   * - Track-to-session associations
   * - Track metadata within session context
   * - Temporal ordering within sessions
   * 
   * Storage Strategy:
   * - Parquet format with Gzip compression for large datasets
   * - Dual partitioning: userId + session_date for optimal queries
   * - Maintains referential integrity with session details
   * 
   * @param eventsStream Distributed stream of listening events
   * @param outputPath Path for session-track relationships output
   * @return Try indicating success or failure
   */
  def persistSessionTrackRelationships(eventsStream: EventStream, outputPath: String): Try[Unit] = {
    require(eventsStream != null, "eventsStream cannot be null")
    require(outputPath != null && outputPath.nonEmpty, "outputPath cannot be null or empty")
    
    try {
      println(s"🔗 Generating session-track relationships: $outputPath")
      
      // Add session IDs to all events
      val sparkStream = eventsStream.asInstanceOf[SparkEventStream]
      val eventsWithSessionsDF = addSessionIdsToEvents(sparkStream.df)
      
      // Add session date for temporal partitioning
      val relationshipsDF = eventsWithSessionsDF
        .withColumn("session_date", date_format($"timestamp", "yyyy-MM-dd"))
        .select(
          $"sessionId",
          $"userId",
          $"timestamp",
          $"artistId",
          $"artistName",
          $"trackId", 
          $"trackName",
          $"trackKey",
          $"session_date"
        )
      
      // Write with dual partitioning for optimal queries
      relationshipsDF
        .write
        .mode("overwrite")
        .option("compression", "gzip")
        .partitionBy("userId", "session_date")
        .parquet(outputPath)
      
      val relationshipCount = relationshipsDF.count()
      println(s"✅ Session-track relationships persisted: $relationshipCount track-session mappings")
      
      Success(())
      
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Failed to persist session-track relationships: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Generates comprehensive session analytics with business insights.
   * 
   * Creates advanced analytics including:
   * - Session length distribution analysis
   * - User engagement patterns
   * - Temporal listening patterns
   * - Content diversity metrics
   * 
   * @param eventsStream Distributed stream of listening events
   * @param outputPath Path for session analytics output
   * @return Try containing SessionAnalytics or failure
   */
  def generateSessionAnalytics(eventsStream: EventStream, outputPath: String): Try[SessionAnalytics] = {
    require(eventsStream != null, "eventsStream cannot be null")
    require(outputPath != null && outputPath.nonEmpty, "outputPath cannot be null or empty")
    
    try {
      println(s"📊 Generating session analytics: $outputPath")
      
      val sparkStream = eventsStream.asInstanceOf[SparkEventStream]
      val sessionsDF = calculateSessionsWithMetadata(sparkStream.df)
      
      // Calculate session length distribution
      val sessionLengthDistribution = calculateSessionLengthDistribution(sessionsDF)
      
      // Calculate user engagement metrics
      val userEngagementMetrics = calculateUserEngagementMetrics(sessionsDF)
      
      // Calculate temporal patterns
      val temporalPatterns = calculateTemporalPatterns(sparkStream.df)
      
      // Calculate quality metrics
      val qualityMetrics = calculateSessionQualityMetrics(sessionsDF)
      
      // Create comprehensive analytics
      val analytics = SessionAnalytics(
        sessionLengthDistribution = sessionLengthDistribution,
        userEngagementMetrics = userEngagementMetrics,
        temporalPatterns = temporalPatterns,
        qualityMetrics = qualityMetrics
      )
      
      // Persist analytics as Parquet
      persistSessionAnalytics(analytics, outputPath)
      
      println(s"✅ Session analytics generated with ${analytics.sessionLengthDistribution.size} distribution buckets")
      
      Success(analytics)
      
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Failed to generate session analytics: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Calculates sessions with comprehensive metadata using window functions.
   */
  private def calculateSessionsWithMetadata(eventsDF: DataFrame): DataFrame = {
    val sessionGapSeconds = 20 * 60 // 20 minutes
    
    // Window specification for session calculation
    val userWindow = Window.partitionBy("userId").orderBy("timestamp")
    
    // Step 1: Detect session boundaries
    val withSessionBoundaries = eventsDF
      .withColumn("prevTimestamp", lag("timestamp", 1).over(userWindow))
      .withColumn("timeGapSeconds",
        when($"prevTimestamp".isNull, 0L)
        .otherwise(unix_timestamp($"timestamp") - unix_timestamp($"prevTimestamp")))
      .withColumn("isNewSession", $"timeGapSeconds" > sessionGapSeconds)
      .withColumn("sessionBoundary", when($"isNewSession", 1).otherwise(0))
    
    // Step 2: Generate session IDs
    val withSessionIds = withSessionBoundaries
      .withColumn("sessionNumber", sum("sessionBoundary").over(userWindow))
      .withColumn("sessionId", concat($"userId", lit("_session_"), $"sessionNumber"))
    
    // Step 3: Calculate comprehensive session metrics
    withSessionIds
      .groupBy("sessionId", "userId")
      .agg(
        min("timestamp").as("startTime"),
        max("timestamp").as("endTime"),
        count(lit(1)).as("trackCount"),
        countDistinct("trackKey").as("uniqueTracks"),
        ((unix_timestamp(max($"timestamp")) - unix_timestamp(min($"timestamp"))) / 60.0).as("durationMinutes"),
        collect_list(struct($"timestamp", $"artistName", $"trackName", $"trackKey")).as("tracks")
      )
      .withColumn("sessionQualityScore", 
        when($"trackCount" >= 10, 100.0)
        .when($"trackCount" >= 5, 80.0)
        .when($"trackCount" >= 2, 60.0)
        .otherwise(40.0))
      .filter($"trackCount" > 0) // Filter out empty sessions
  }
  
  /**
   * Adds session IDs to events for relationship mapping.
   */
  private def addSessionIdsToEvents(eventsDF: DataFrame): DataFrame = {
    val sessionGapSeconds = 20 * 60
    val userWindow = Window.partitionBy("userId").orderBy("timestamp")
    
    eventsDF
      .withColumn("prevTimestamp", lag("timestamp", 1).over(userWindow))
      .withColumn("timeGapSeconds",
        when($"prevTimestamp".isNull, 0L)
        .otherwise(unix_timestamp($"timestamp") - unix_timestamp($"prevTimestamp")))
      .withColumn("isNewSession", $"timeGapSeconds" > sessionGapSeconds)
      .withColumn("sessionBoundary", when($"isNewSession", 1).otherwise(0))
      .withColumn("sessionNumber", sum("sessionBoundary").over(userWindow))
      .withColumn("sessionId", concat($"userId", lit("_session_"), $"sessionNumber"))
  }
  
  /**
   * Calculates session length distribution for analytics.
   */
  private def calculateSessionLengthDistribution(sessionsDF: DataFrame): Map[String, Long] = {
    val distributionDF = sessionsDF
      .withColumn("sessionLengthBucket",
        when($"trackCount" <= 1, "1 track")
        .when($"trackCount" <= 5, "2-5 tracks")
        .when($"trackCount" <= 10, "6-10 tracks")
        .when($"trackCount" <= 20, "11-20 tracks")
        .when($"trackCount" <= 50, "21-50 tracks")
        .otherwise("50+ tracks"))
      .groupBy("sessionLengthBucket")
      .agg(count(lit(1)).as("sessionCount"))
      .collect()
    
    distributionDF.map(row => 
      row.getAs[String]("sessionLengthBucket") -> row.getAs[Long]("sessionCount")
    ).toMap
  }
  
  /**
   * Calculates user engagement metrics.
   */
  private def calculateUserEngagementMetrics(sessionsDF: DataFrame): Map[String, Double] = {
    val engagementDF = sessionsDF
      .groupBy("userId")
      .agg(
        count(lit(1)).as("sessionCount"),
        avg("trackCount").as("avgSessionLength"),
        sum("trackCount").as("totalTracks"),
        avg("durationMinutes").as("avgSessionDuration")
      )
      .agg(
        avg("sessionCount").as("avgSessionsPerUser"),
        avg("avgSessionLength").as("avgTracksPerSession"),
        avg("totalTracks").as("avgTracksPerUser"),
        avg("avgSessionDuration").as("avgSessionDurationMinutes")
      )
      .collect()
      .head
    
    Map(
      "avgSessionsPerUser" -> engagementDF.getAs[Double]("avgSessionsPerUser"),
      "avgTracksPerSession" -> engagementDF.getAs[Double]("avgTracksPerSession"),
      "avgTracksPerUser" -> engagementDF.getAs[Double]("avgTracksPerUser"),
      "avgSessionDurationMinutes" -> engagementDF.getAs[Double]("avgSessionDurationMinutes")
    )
  }
  
  /**
   * Calculates temporal listening patterns.
   */
  private def calculateTemporalPatterns(eventsDF: DataFrame): Map[String, Long] = {
    val temporalDF = eventsDF
      .withColumn("hour", hour($"timestamp"))
      .withColumn("dayOfWeek", dayofweek($"timestamp"))
      .groupBy("hour")
      .agg(count(lit(1)).as("eventCount"))
      .collect()
    
    temporalDF.map(row =>
      f"hour_${row.getAs[Int]("hour")}%02d" -> row.getAs[Long]("eventCount")
    ).toMap
  }
  
  /**
   * Calculates session quality metrics for business insights.
   */
  private def calculateSessionQualityMetrics(sessionsDF: DataFrame): SessionMetrics = {
    val metricsRow = sessionsDF
      .agg(
        count(lit(1)).as("totalSessions"),
        countDistinct("userId").as("uniqueUsers"),
        sum("trackCount").as("totalTracks"),
        avg("trackCount").as("averageSessionLength"),
        avg("sessionQualityScore").as("qualityScore")
      )
      .collect()
      .head
    
    SessionMetrics(
      totalSessions = metricsRow.getAs[Long]("totalSessions"),
      uniqueUsers = metricsRow.getAs[Long]("uniqueUsers"),
      totalTracks = metricsRow.getAs[Long]("totalTracks"),
      averageSessionLength = metricsRow.getAs[Double]("averageSessionLength"),
      qualityScore = metricsRow.getAs[Double]("qualityScore")
    )
  }
  
  /**
   * Persists session analytics as structured data.
   */
  private def persistSessionAnalytics(analytics: SessionAnalytics, outputPath: String): Unit = {
    // Convert analytics to DataFrames for persistence
    
    // Session length distribution
    val distributionDF = analytics.sessionLengthDistribution.toSeq.toDF("sessionLengthBucket", "sessionCount")
    distributionDF
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(s"$outputPath/session-length-distribution")
    
    // User engagement metrics
    val engagementDF = analytics.userEngagementMetrics.toSeq.toDF("metric", "value")
    engagementDF
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(s"$outputPath/user-engagement-metrics")
    
    // Temporal patterns
    val temporalDF = analytics.temporalPatterns.toSeq.toDF("timePattern", "eventCount")
    temporalDF
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(s"$outputPath/temporal-patterns")
    
    // Quality metrics
    val qualityDF = Seq(analytics.qualityMetrics).toDF()
    qualityDF
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(s"$outputPath/quality-metrics")
  }
}

/**
 * Companion object for factory methods and utilities.
 */
object CompleteSessionDatasetGenerator {
  
  /**
   * Creates generator with production configuration.
   */
  def createProduction(implicit spark: SparkSession): CompleteSessionDatasetGenerator = {
    new CompleteSessionDatasetGenerator()
  }
  
  /**
   * Creates generator for testing.
   */
  def createForTesting(implicit spark: SparkSession): CompleteSessionDatasetGenerator = {
    new CompleteSessionDatasetGenerator()
  }
}

/**
 * Comprehensive session analytics data structure.
 * 
 * Contains business insights and patterns derived from session analysis.
 * 
 * @param sessionLengthDistribution Distribution of sessions by track count
 * @param userEngagementMetrics User engagement patterns and statistics
 * @param temporalPatterns Temporal listening activity patterns
 * @param qualityMetrics Session data quality metrics
 */
case class SessionAnalytics(
  sessionLengthDistribution: Map[String, Long],
  userEngagementMetrics: Map[String, Double],
  temporalPatterns: Map[String, Long],
  qualityMetrics: SessionMetrics
) {
  require(sessionLengthDistribution != null, "sessionLengthDistribution cannot be null")
  require(userEngagementMetrics != null, "userEngagementMetrics cannot be null")
  require(temporalPatterns != null, "temporalPatterns cannot be null")
  require(qualityMetrics != null, "qualityMetrics cannot be null")
  
  /**
   * Determines if analytics indicate high user engagement.
   */
  def indicatesHighEngagement: Boolean = {
    userEngagementMetrics.get("avgSessionsPerUser").exists(_ > 50.0) &&
    userEngagementMetrics.get("avgTracksPerSession").exists(_ > 20.0)
  }
  
  /**
   * Identifies peak listening hour from temporal patterns.
   */
  def peakListeningHour: Option[Int] = {
    temporalPatterns
      .filter(_._1.startsWith("hour_"))
      .maxByOption(_._2)
      .map(_._1.replace("hour_", "").toInt)
  }
}
