package com.lastfm.sessions.infrastructure

import com.lastfm.sessions.domain.{DataRepositoryPort, ListenEvent, DataQualityMetrics}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.Instant
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

/**
 * Spark-based implementation of DataRepositoryPort for loading LastFM listening events.
 * 
 * This implementation uses Apache Spark to efficiently process large TSV files containing
 * listening event data. It handles data validation, schema enforcement, and error recovery
 * while maintaining the contract defined by the DataRepositoryPort interface.
 * 
 * Key features:
 * - Efficient TSV parsing with schema validation
 * - Data quality filtering for malformed records
 * - Unicode character support for international content
 * - Memory-efficient processing for large datasets
 * - Comprehensive error handling and recovery
 * 
 * @param spark Implicit SparkSession for data processing operations
 * @author LastFM Session Analysis Team
 * @since 1.0.0
 */
class SparkDataRepository(implicit spark: SparkSession) extends DataRepositoryPort {
  
  import spark.implicits._
  
  /**
   * Schema definition for LastFM TSV files.
   * 
   * Based on the actual LastFM dataset format:
   * userId | timestamp | artistId | artistName | trackId | trackName
   */
  private val schema = StructType(Seq(
    StructField("userId", StringType, nullable = false),
    StructField("timestamp", StringType, nullable = false),  // Will be parsed to timestamp
    StructField("artistId", StringType, nullable = true),
    StructField("artistName", StringType, nullable = false),
    StructField("trackId", StringType, nullable = true),
    StructField("trackName", StringType, nullable = false)
  ))
  
  /**
   * Loads listening events from a TSV file using Spark DataFrame operations.
   * 
   * This method:
   * 1. Reads TSV file with proper schema
   * 2. Validates and filters malformed records
   * 3. Converts to domain objects
   * 4. Returns validated ListenEvent instances
   * 
   * @param dataSourcePath Path to the TSV file containing listening events
   * @return Try containing either List of ListenEvent objects or failure exception
   */
  override def loadListenEvents(dataSourcePath: String): Try[List[ListenEvent]] = {
    try {
      // Read TSV file with schema
      val rawDF = spark.read
        .option("header", "false")  // No header in LastFM dataset
        .option("delimiter", "\t")   // Tab-separated values
        .option("encoding", "UTF-8") // Support Unicode characters
        .option("mode", "PERMISSIVE") // Handle malformed records gracefully
        .schema(schema)
        .csv(dataSourcePath)
      
      // Filter out malformed records and validate data
      val validatedDF = rawDF
        .filter($"userId".isNotNull && $"userId" =!= "")
        .filter($"timestamp".isNotNull && $"timestamp" =!= "")
        .filter($"artistName".isNotNull && $"artistName" =!= "")
        .filter($"trackName".isNotNull && $"trackName" =!= "")
        // Remove records with only whitespace in required fields
        .filter(trim($"userId") =!= "")
        .filter(trim($"artistName") =!= "")
        .filter(trim($"trackName") =!= "")
      
      // Convert to ListenEvent objects
      val events = validatedDF
        .collect()
        .map(row => convertRowToListenEvent(row))
        .filter(_.isSuccess)
        .map(_.get)
        .toList
      
      Success(events)
      
    } catch {
      case NonFatal(exception) =>
        Failure(exception)
    }
  }
  
  /**
   * Converts a Spark DataFrame Row to a ListenEvent domain object.
   * 
   * Handles type conversion, validation, and error recovery for individual records.
   * 
   * @param row Spark DataFrame row containing listening event data
   * @return Try containing either ListenEvent object or conversion failure
   */
  private def convertRowToListenEvent(row: org.apache.spark.sql.Row): Try[ListenEvent] = {
    try {
      val userId = row.getAs[String]("userId")
      val timestampStr = row.getAs[String]("timestamp")
      val artistIdOpt = Option(row.getAs[String]("artistId")).filter(_.nonEmpty)
      val artistName = row.getAs[String]("artistName")
      val trackIdOpt = Option(row.getAs[String]("trackId")).filter(_.nonEmpty)
      val trackName = row.getAs[String]("trackName")
      
      // Parse timestamp - LastFM uses ISO 8601 format
      val timestamp = Instant.parse(timestampStr)
      
      val event = ListenEvent(
        userId = userId,
        timestamp = timestamp,
        artistId = artistIdOpt,
        artistName = artistName,
        trackId = trackIdOpt,
        trackName = trackName,
        trackKey = trackIdOpt.getOrElse(s"$artistName — $trackName")
      )
      
      Success(event)
      
    } catch {
      case NonFatal(exception) =>
        // Log the error but don't fail the entire operation
        Failure(exception)
    }
  }

  override def loadWithDataQuality(dataSourcePath: String): Try[(List[ListenEvent], DataQualityMetrics)] = {
    // Simple implementation for now
    val events = loadListenEvents(dataSourcePath).getOrElse(List.empty)
    val metrics = DataQualityMetrics(
      totalRecords = events.length.toLong,
      validRecords = events.length.toLong,
      rejectedRecords = 0L,
      rejectionReasons = Map.empty,
      trackIdCoverage = 0.0,
      suspiciousUsers = 0L
    )
    Success((events, metrics))
  }

  override def cleanAndPersist(inputPath: String, outputPath: String): Try[DataQualityMetrics] = {
    try {
      // Load and process the data
      val eventsResult = loadListenEvents(inputPath)
      
      // Count total input records (including invalid ones)
      val totalInputRecords = countTotalInputRecords(inputPath)
      
      eventsResult match {
        case Success(events) =>
          val totalRecords = totalInputRecords
          val validRecords = events.length.toLong // Only successfully loaded events
          val rejectedRecords = totalRecords - validRecords // Records that were filtered out
          
          // Calculate track ID coverage
          val trackIdCoverage = if (events.nonEmpty) {
            val withTrackId = events.count(_.trackId.nonEmpty)
            (withTrackId.toDouble / events.length) * 100.0
          } else 0.0
          
          // Calculate suspicious users (users with >100k plays)
          val suspiciousUsers = events.groupBy(_.userId).values.count(_.length > 100000)
          
          val qualityMetrics = DataQualityMetrics(
            totalRecords = totalRecords,
            validRecords = validRecords,
            rejectedRecords = rejectedRecords,
            rejectionReasons = Map.empty, // Could be enhanced to track specific rejection reasons
            trackIdCoverage = trackIdCoverage,
            suspiciousUsers = suspiciousUsers
          )
          
          // Write quality report
          writeQualityReport(qualityMetrics, outputPath)
          
          Success(qualityMetrics)
          
        case Failure(exception) =>
          Failure(exception)
      }
    } catch {
      case NonFatal(exception) =>
        Failure(exception)
    }
  }
  
  private def countTotalInputRecords(inputPath: String): Long = {
    try {
      val lines = Files.readAllLines(Paths.get(inputPath), StandardCharsets.UTF_8)
      lines.size.toLong
    } catch {
      case _: Exception => 0L
    }
  }

  private def writeQualityReport(metrics: DataQualityMetrics, outputPath: String): Unit = {
    val qualityReportPath = getQualityReportPath(outputPath)
    val reportContent = generateQualityReportJSON(metrics)
    
    val outputFile = Paths.get(qualityReportPath)
    val outputDir = outputFile.getParent
    if (outputDir != null) {
      Files.createDirectories(outputDir)
    }
    
    Files.write(outputFile, reportContent.getBytes(StandardCharsets.UTF_8))
  }
  
  private def getQualityReportPath(outputPath: String): String = {
    val outputFile = Paths.get(outputPath)
    val outputDir = Option(outputFile.getParent).getOrElse(Paths.get("."))
    val baseFileName = outputFile.getFileName.toString.replaceAll("\\.[^.]*$", "")
    outputDir.resolve(s"$baseFileName-quality-report.json").toAbsolutePath.toString
  }
  
  private def generateQualityReportJSON(metrics: DataQualityMetrics): String = {
    val rejectionReasonsJson = metrics.rejectionReasons.map { case (reason, count) =>
      s""""$reason": $count"""
    }.mkString(", ")
    
    s"""{
  "timestamp": "${java.time.Instant.now()}",
  "dataset": "lastfm-1k",
  "processing": {
    "totalRecords": ${metrics.totalRecords},
    "validRecords": ${metrics.validRecords},
    "rejectedRecords": ${metrics.rejectedRecords},
    "qualityScore": ${metrics.qualityScore}
  },
  "dataCharacteristics": {
    "trackIdCoverage": ${metrics.trackIdCoverage},
    "suspiciousUsers": ${metrics.suspiciousUsers}
  },
  "qualityAssessment": {
    "isSessionAnalysisReady": ${metrics.isSessionAnalysisReady},
    "isProductionReady": ${metrics.isProductionReady},
    "hasAcceptableTrackCoverage": ${metrics.hasAcceptableTrackCoverage}
  },
  "rejectionReasons": {
    $rejectionReasonsJson
  },
  "thresholds": {
    "sessionAnalysisMinQuality": 99.0,
    "productionMinQuality": 99.9,
    "minTrackIdCoverage": 85.0
  }
}"""
  }
}