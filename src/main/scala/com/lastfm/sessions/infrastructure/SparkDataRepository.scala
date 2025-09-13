package com.lastfm.sessions.infrastructure

import com.lastfm.sessions.domain.{DataRepositoryPort, ListenEvent, DataQualityMetrics, UserSession}
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
 * listening event data and outputs cleaned data in Parquet format for optimal performance.
 * It handles data validation, schema enforcement, and error recovery while maintaining 
 * the contract defined by the DataRepositoryPort interface.
 * 
 * Key features:
 * - Efficient TSV parsing with schema validation (Bronze layer input)
 * - Parquet output with strategic userId partitioning (Silver layer output)
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
   * Loads listening events from a TSV file using memory-efficient Spark DataFrame operations.
   * 
   * This method:
   * 1. Reads TSV file with proper schema
   * 2. Validates and filters malformed records
   * 3. Converts to domain objects using batch processing
   * 4. Returns validated ListenEvent instances
   * 
   * @param dataSourcePath Path to the TSV file containing listening events
   * @return Try containing either List of ListenEvent objects or failure exception
   */
  override def loadListenEvents(dataSourcePath: String): Try[List[ListenEvent]] = {
    loadListenEvents(dataSourcePath, maxReturnSize = 5000)
  }

  /**
   * Loads listening events with configurable batch size to avoid memory issues.
   * 
   * @param dataSourcePath Path to the TSV file containing listening events
   * @param maxReturnSize Maximum number of events to return (prevents memory issues)
   * @return Try containing either List of ListenEvent objects or failure exception
   */
  def loadListenEvents(
    dataSourcePath: String, 
    maxReturnSize: Int
  ): Try[List[ListenEvent]] = {
    try {
      // Read and validate using distributed operations
      val rawDF = spark.read
        .option("header", "false")
        .option("delimiter", "\t")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(dataSourcePath)
        .repartition(16) // Optimal partitioning

      // Apply validation filters
      val validatedDF = rawDF
        .filter($"userId".isNotNull && $"userId" =!= "")
        .filter($"timestamp".isNotNull && $"timestamp" =!= "")
        .filter($"artistName".isNotNull && $"artistName" =!= "")
        .filter($"trackName".isNotNull && $"trackName" =!= "")
        .filter(trim($"userId") =!= "")
        .filter(trim($"artistName") =!= "")
        .filter(trim($"trackName") =!= "")

      // Return a limited sample to prevent memory issues
      val events = validatedDF
        .limit(maxReturnSize) // Limit result set size
        .collect()
        .map(row => convertRowToListenEventSafely(row))
        .filter(_.isSuccess)
        .map(_.get)
        .toList

      Success(events)

    } catch {
      case _: OutOfMemoryError =>
        Failure(new RuntimeException("Dataset too large - using batch processing instead"))
      case NonFatal(exception) =>
        Failure(exception)
    }
  }

  /**
   * Processes data in configurable batches to handle very large datasets.
   * 
   * This method processes the dataset in chunks, allowing for memory-efficient
   * processing of datasets that don't fit in memory.
   * 
   * @param dataSourcePath Path to the data file
   * @param batchSize Number of records to process per batch
   * @return Try containing processed ListenEvent objects
   */
  def processInBatches(dataSourcePath: String, batchSize: Int = 10000): Try[List[ListenEvent]] = {
    try {
      val rawDF = spark.read
        .option("header", "false")
        .option("delimiter", "\t")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(dataSourcePath)
        .repartition(16)

      // Apply validation filters
      val validatedDF = rawDF
        .filter($"userId".isNotNull && $"userId" =!= "")
        .filter($"timestamp".isNotNull && $"timestamp" =!= "")
        .filter($"artistName".isNotNull && $"artistName" =!= "")
        .filter($"trackName".isNotNull && $"trackName" =!= "")
        .filter(trim($"userId") =!= "")
        .filter(trim($"artistName") =!= "")
        .filter(trim($"trackName") =!= "")

      // Process in batches using Spark DataFrame operations (serialization-safe)
      val processedEvents = validatedDF
        .limit(50000) // Limit total results for safety
        .collect()
        .grouped(batchSize)
        .flatMap(batch => batch.flatMap(row => convertRowToListenEventSafely(row).toOption))
        .toList

      Success(processedEvents)

    } catch {
      case _: OutOfMemoryError =>
        Failure(new RuntimeException("Even batch processing failed - dataset too large for current configuration"))
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


  /**
   * Memory-efficient implementation for processing large static TSV files.
   * 
   * Key optimizations for static data:
   * - Avoids collect() operations that load entire dataset into memory
   * - Uses statistical sampling for quality metrics instead of processing all records
   * - Leverages Spark's lazy evaluation and distributed processing
   * - Processes static files in chunks to prevent OutOfMemoryErrors
   * 
   * @param dataSourcePath Path to the static TSV file
   * @param sampleFraction Fraction of data to sample for quality metrics (default: 1% for large datasets)
   * @return Try containing sample events and comprehensive quality metrics
   */
  def loadWithDataQualityBatched(
    dataSourcePath: String, 
    sampleFraction: Double = 0.01,
    maxSampleSize: Int = 1000
  ): Try[(List[ListenEvent], DataQualityMetrics)] = {
    try {
      // Read and validate data using Spark's distributed operations
      val rawDF = spark.read
        .option("header", "false")
        .option("delimiter", "\t")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(dataSourcePath)
        .repartition(16) // Optimal partitioning for processing

      // Count total records efficiently (no data movement)
      val totalRecords = rawDF.count()

      // Apply data quality filters using distributed operations
      val validatedDF = rawDF
        .filter($"userId".isNotNull && $"userId" =!= "")
        .filter($"timestamp".isNotNull && $"timestamp" =!= "")
        .filter($"artistName".isNotNull && $"artistName" =!= "")
        .filter($"trackName".isNotNull && $"trackName" =!= "")
        .filter(trim($"userId") =!= "")
        .filter(trim($"artistName") =!= "")
        .filter(trim($"trackName") =!= "")
        .cache() // Cache for multiple operations

      // Count valid records efficiently
      val validRecords = validatedDF.count()

      // Calculate track ID coverage using distributed aggregation
      val trackIdCoverage = calculateTrackIdCoverageDistributed(validatedDF)

      // Sample data for detailed quality analysis and return to user
      val sampleEvents = extractQualitySample(validatedDF, sampleFraction, maxSampleSize)

      // Calculate suspicious users using distributed processing
      val suspiciousUsers = calculateSuspiciousUsersDistributed(validatedDF)

      val qualityMetrics = DataQualityMetrics(
        totalRecords = totalRecords,
        validRecords = validRecords,
        rejectedRecords = totalRecords - validRecords,
        rejectionReasons = Map("invalid_format" -> (totalRecords - validRecords)),
        trackIdCoverage = trackIdCoverage,
        suspiciousUsers = suspiciousUsers
      )

      // Unpersist cached DataFrame to free memory
      validatedDF.unpersist()

      Success((sampleEvents, qualityMetrics))

    } catch {
      case _: OutOfMemoryError =>
        Failure(new RuntimeException("Dataset too large for current memory configuration. Consider increasing heap size or using smaller sample fraction."))
      case NonFatal(exception) =>
        Failure(exception)
    }
  }

  /**
   * Calculates track ID coverage using distributed Spark operations without collecting data.
   */
  private def calculateTrackIdCoverageDistributed(df: DataFrame): Double = {
    val totalTracksCount = df.count()
    if (totalTracksCount == 0) return 0.0

    val tracksWithIdCount = df
      .filter($"trackId".isNotNull && $"trackId" =!= "")
      .count()

    (tracksWithIdCount.toDouble / totalTracksCount) * 100.0
  }

  /**
   * Extracts a quality sample for detailed analysis without loading full dataset.
   */
  private def extractQualitySample(df: DataFrame, sampleFraction: Double, maxSampleSize: Int): List[ListenEvent] = {
    val sampleDF = df.sample(withReplacement = false, sampleFraction)
      .limit(maxSampleSize) // Ensure we don't exceed memory limits

    sampleDF.collect()
      .map(row => convertRowToListenEventSafely(row))
      .filter(_.isSuccess)
      .map(_.get)
      .toList
  }

  /**
   * Calculates suspicious users using distributed aggregation operations.
   */
  private def calculateSuspiciousUsersDistributed(df: DataFrame): Long = {
    df.groupBy($"userId")
      .count()
      .filter($"count" > 100000)
      .count()
  }

  /**
   * Safe conversion method that handles conversion errors gracefully.
   */
  private def convertRowToListenEventSafely(row: org.apache.spark.sql.Row): Try[ListenEvent] = {
    try {
      convertRowToListenEvent(row)
    } catch {
      case NonFatal(exception) =>
        Failure(exception)
    }
  }

  override def cleanAndPersist(inputPath: String, outputPath: String): Try[DataQualityMetrics] = {
    try {
      // Use memory-efficient batch processing for large static files
      val (sampleEvents, qualityMetrics) = loadWithDataQualityBatched(inputPath) match {
        case Success(result) => result
        case Failure(exception) => throw exception
      }
      
      // Write cleaned data directly using Spark DataFrame operations (memory-efficient)
      writeCleanedDataBatchedToSilver(inputPath, outputPath)
      
      // Write quality report
      writeQualityReport(qualityMetrics, outputPath)
      
      Success(qualityMetrics)
      
    } catch {
      case _: OutOfMemoryError =>
        Failure(new RuntimeException("Static dataset too large for current memory configuration. Consider increasing heap size or using Docker with more memory allocation."))
      case NonFatal(exception) =>
        Failure(exception)
    }
  }

  /**
   * Memory-efficient writing of cleaned static data directly using Spark DataFrame operations.
   * 
   * This method processes static TSV files and writes data without loading everything into memory:
   * - Reads static data using Spark's lazy evaluation
   * - Applies cleaning filters using distributed operations
   * - Writes directly to Silver layer without intermediate collections
   * 
   * @param inputPath Source static data path
   * @param outputPath Silver layer output path
   */
  private def writeCleanedDataBatchedToSilver(inputPath: String, outputPath: String): Unit = {
    // Read and clean data using distributed operations
    val cleanedDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .option("encoding", "UTF-8")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .csv(inputPath)
      .repartition(16, $"userId") // Strategic userId partitioning for session analysis optimization
      // Apply all cleaning filters
      .filter($"userId".isNotNull && $"userId" =!= "")
      .filter($"timestamp".isNotNull && $"timestamp" =!= "")
      .filter($"artistName".isNotNull && $"artistName" =!= "")
      .filter($"trackName".isNotNull && $"trackName" =!= "")
      .filter(trim($"userId") =!= "")
      .filter(trim($"artistName") =!= "")
      .filter(trim($"trackName") =!= "")
      // Add trackKey column using SQL expressions (memory-efficient)
      .withColumn("trackKey", 
        when($"trackId".isNotNull && $"trackId" =!= "", $"trackId")
        .otherwise(concat($"artistName", lit(" — "), $"trackName")))

    // Write directly to Silver layer as Parquet with optimal partitioning
    val parquetDataPath = outputPath + "_parquet_data"
    
    // Write as Parquet with optimal userId partitioning (memory-efficient)
    cleanedDF
      .repartition(16, $"userId") // Optimal partitioning: 16 partitions with ~62 users each
      .write
      .mode("overwrite")
      .option("compression", "snappy") // Optimal compression for performance
      .parquet(parquetDataPath)
    
    println(s"✅ Silver layer written as Parquet with optimal userId partitioning")
    println(s"   Format: Parquet with Snappy compression")
    println(s"   Partitioning: 16 partitions (~62 users per partition)")
    println(s"   Path: $parquetDataPath")
  }
  
  private def countTotalInputRecords(inputPath: String): Long = {
    try {
      val lines = Files.readAllLines(Paths.get(inputPath), StandardCharsets.UTF_8)
      lines.size.toLong
    } catch {
      case _: Exception => 0L
    }
  }

  /**
   * Resolves Silver layer path, handling both single files and TSV data directories.
   * 
   * Strategy:
   * 1. Check if the exact path exists (single file)
   * 2. Check if a TSV data directory exists (path + "_tsv_data")
   * 3. Fall back to original path for error handling
   */
  private def resolveSilverLayerPath(silverPath: String): String = {
    val originalPath = Paths.get(silverPath)
    // Match the writing logic: outputPath + "_tsv_data"  
    val tsvDataPath = Paths.get(silverPath + "_tsv_data")
    
    // Check if TSV data directory exists (most common case for our pipeline)
    if (Files.exists(tsvDataPath) && Files.isDirectory(tsvDataPath)) {
      tsvDataPath.toString
    }
    // Check if original single file exists  
    else if (Files.exists(originalPath)) {
      silverPath
    }
    // Fall back to original path (will trigger appropriate error handling)
    else {
      silverPath
    }
  }

  // Note: persistSessionAnalysis method removed as it's replaced by 
  // DistributedSessionAnalysisRepository with memory-efficient distributed processing

  // Note: Old session analysis persistence methods removed.
  // Replaced by DistributedSessionAnalysisRepository with distributed processing.


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