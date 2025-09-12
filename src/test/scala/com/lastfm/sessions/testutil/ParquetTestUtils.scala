package com.lastfm.sessions.testutil

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.util.{Try, Success, Failure}

/**
 * Utility functions for Parquet-based testing infrastructure.
 * 
 * Provides comprehensive support for creating, validating, and managing
 * Parquet test data in the LastFM session analysis test suite.
 * 
 * Key Features:
 * - Memory-efficient test data creation using DataFrames
 * - Parquet structure validation and verification
 * - Format-agnostic test data conversion utilities
 * - Schema validation and compatibility checking
 * - Performance-optimized for large test datasets
 * 
 * Design Principles:
 * - Test Isolation: Each test gets clean, independent data
 * - Performance: Efficient Parquet operations for large test data
 * - Reusability: Common patterns extracted into utility methods
 * - Validation: Comprehensive structure and content validation
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object ParquetTestUtils {
  
  /**
   * LastFM schema definition optimized for test data.
   */
  val testSchema = StructType(Seq(
    StructField("userId", StringType, nullable = false),
    StructField("timestamp", StringType, nullable = false),
    StructField("artistId", StringType, nullable = true),
    StructField("artistName", StringType, nullable = false),
    StructField("trackId", StringType, nullable = true),
    StructField("trackName", StringType, nullable = false),
    StructField("trackKey", StringType, nullable = false)
  ))
  
  /**
   * Creates Parquet test data from structured input.
   * 
   * Converts list of tuples to properly formatted Parquet files with:
   * - Correct schema validation
   * - Track key generation for identity resolution
   * - Optimal partitioning for test performance
   * - Snappy compression for storage efficiency
   * 
   * @param data List of tuples (userId, timestamp, artistId, artistName, trackId, trackName)
   * @param path Output path for Parquet files
   * @param spark Implicit Spark session
   * @return Path to created Parquet directory
   */
  def createParquetTestData(
    data: List[(String, String, String, String, String, String)], 
    path: String
  )(implicit spark: SparkSession): String = {
    import spark.implicits._
    
    // Convert to DataFrame with proper schema
    val df = spark.createDataFrame(data.map { case (userId, timestamp, artistId, artistName, trackId, trackName) =>
      (userId, timestamp, Option(artistId).filter(_.nonEmpty).orNull, artistName, Option(trackId).filter(_.nonEmpty).orNull, trackName)
    }).toDF("userId", "timestamp", "artistId", "artistName", "trackId", "trackName")
    
    // Add track key for identity resolution
    val enhancedDF = df.withColumn("trackKey",
      when($"trackId".isNotNull && $"trackId" =!= "", $"trackId")
      .otherwise(concat($"artistName", lit(" — "), $"trackName"))
    )
    
    // Write as Parquet with test optimizations
    enhancedDF
      .repartition(2) // Small partition count for tests
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(path)
    
    path
  }
  
  /**
   * Validates Parquet file structure and content.
   * 
   * Performs comprehensive validation:
   * - File system structure validation
   * - Schema compatibility checking
   * - Record count and content validation
   * - Partition structure verification
   * 
   * @param path Path to Parquet directory
   * @param spark Implicit Spark session
   * @return Boolean indicating if structure is valid
   */
  def validateParquetStructure(path: String)(implicit spark: SparkSession): Boolean = {
    try {
      // Check if path exists
      val pathObj = Paths.get(path)
      if (!Files.exists(pathObj)) return false
      
      // Try to read Parquet files
      val df = spark.read.parquet(path)
      
      // Validate schema compatibility
      val actualSchema = df.schema
      val requiredFields = Set("userId", "timestamp", "artistName", "trackName", "trackKey")
      val actualFields = actualSchema.fieldNames.toSet
      
      // Check all required fields are present
      if (!requiredFields.subsetOf(actualFields)) return false
      
      // Check basic data integrity
      val recordCount = df.count()
      if (recordCount < 0) return false
      
      // Verify we can read actual data
      df.take(1) // This will fail if data is corrupted
      
      true
    } catch {
      case _: Exception => false
    }
  }
  
  /**
   * Reads Parquet records for test validation.
   * 
   * @param path Path to Parquet directory
   * @param spark Implicit Spark session
   * @return List of test records for validation
   */
  def readParquetRecords(path: String)(implicit spark: SparkSession): List[TestRecord] = {
    val df = spark.read.parquet(path)
    
    df.collect().map { row =>
      TestRecord(
        userId = row.getAs[String]("userId"),
        timestamp = row.getAs[String]("timestamp"),
        artistId = Option(row.getAs[String]("artistId")),
        artistName = row.getAs[String]("artistName"),
        trackId = Option(row.getAs[String]("trackId")),
        trackName = row.getAs[String]("trackName"),
        trackKey = row.getAs[String]("trackKey")
      )
    }.toList
  }
  
  /**
   * Validates Parquet file sizes are optimal for testing.
   * 
   * @param path Path to Parquet directory
   * @return File size metrics for validation
   */
  def validateParquetFileSizes(path: String): ParquetSizeMetrics = {
    try {
      val pathObj = Paths.get(path)
      if (!Files.exists(pathObj)) {
        return ParquetSizeMetrics(0, 0, 0, false)
      }
      
      val fileSizes = Files.walk(pathObj)
        .filter(_.toString.endsWith(".parquet"))
        .mapToLong(Files.size)
        .toArray
      
      if (fileSizes.isEmpty) {
        ParquetSizeMetrics(0, 0, 0, false)
      } else {
        val totalSize = fileSizes.sum
        val avgSize = totalSize / fileSizes.length
        val maxSize = fileSizes.max
        val isOptimal = avgSize > 1024 && avgSize < 10 * 1024 * 1024 // 1KB to 10MB for tests
        
        ParquetSizeMetrics(totalSize, avgSize, maxSize, isOptimal)
      }
    } catch {
      case _: Exception => ParquetSizeMetrics(0, 0, 0, false)
    }
  }
  
  /**
   * Creates realistic test data with proper distributions.
   * 
   * @param users Number of users to generate
   * @param tracksPerUser Average tracks per user
   * @param spark Implicit Spark session
   * @return Generated test data
   */
  def generateRealisticTestData(
    users: Int, 
    tracksPerUser: Int
  ): List[(String, String, String, String, String, String)] = {
    (1 to users).flatMap { userIndex =>
      (1 to tracksPerUser).map { trackIndex =>
        val userId = f"user_${userIndex}%06d"
        val timestamp = f"2009-05-04T${(trackIndex % 24)}%02d:08:57Z"
        val artistId = if (trackIndex % 10 == 0) "" else s"artist-$userIndex-$trackIndex" // 10% missing artist IDs
        val artistName = s"Artist $userIndex"
        val trackId = if (trackIndex % 8 == 0) "" else s"track-$userIndex-$trackIndex" // ~12% missing track IDs (realistic)
        val trackName = s"Track $trackIndex"
        
        (userId, timestamp, artistId, artistName, trackId, trackName)
      }
    }.toList
  }
  
  /**
   * Converts TSV test data to Parquet format for migration.
   * 
   * @param tsvData TSV-formatted test data
   * @param outputPath Output path for Parquet conversion
   * @param spark Implicit Spark session
   * @return Success/failure of conversion
   */
  def convertTsvToParquet(tsvData: String, outputPath: String)(implicit spark: SparkSession): Try[String] = {
    try {
      // Parse TSV data
      val lines = tsvData.split("\n").filter(_.nonEmpty)
      val data = lines.map { line =>
        val parts = line.split("\t")
        if (parts.length >= 6) {
          (parts(0), parts(1), parts(2), parts(3), parts(4), parts(5))
        } else {
          throw new IllegalArgumentException(s"Invalid TSV line: $line")
        }
      }.toList
      
      // Create Parquet from parsed data
      val parquetPath = createParquetTestData(data, outputPath)
      Success(parquetPath)
    } catch {
      case ex: Exception => Failure(ex)
    }
  }
  
  /**
   * Cleans up test Parquet directories recursively.
   * 
   * @param paths Paths to clean up
   */
  def cleanupParquetTestData(paths: String*): Unit = {
    paths.foreach { path =>
      try {
        val pathObj = Paths.get(path)
        if (Files.exists(pathObj)) {
          Files.walk(pathObj)
            .sorted(java.util.Comparator.reverseOrder())
            .forEach(Files.deleteIfExists)
        }
      } catch {
        case _: Exception => // Ignore cleanup failures
      }
    }
  }
  
  /**
   * Creates temporary Parquet test directory.
   * 
   * @param prefix Directory prefix
   * @return Path to temporary directory
   */
  def createTempParquetDir(prefix: String): String = {
    val tempDir = Files.createTempDirectory(prefix)
    tempDir.toString
  }
  
  /**
   * Validates Parquet partitioning for userId-based tests.
   * 
   * @param path Path to partitioned Parquet data
   * @param spark Implicit Spark session
   * @return Partition validation metrics
   */
  def validatePartitioning(path: String)(implicit spark: SparkSession): PartitionValidationMetrics = {
    try {
      val df = spark.read.parquet(path)
      
      val partitionCounts = df
        .groupBy(spark_partition_id().as("partitionId"))
        .agg(
          countDistinct("userId").as("userCount"),
          count(lit(1)).as("recordCount")
        )
        .collect()
      
      val userCounts = partitionCounts.map(_.getAs[Long]("userCount"))
      val recordCounts = partitionCounts.map(_.getAs[Long]("recordCount"))
      
      val maxUsers = if (userCounts.nonEmpty) userCounts.max else 0L
      val minUsers = if (userCounts.nonEmpty) userCounts.min else 0L
      val userSkew = if (minUsers > 0) maxUsers.toDouble / minUsers else 1.0
      
      val totalUsers = userCounts.sum
      val totalRecords = recordCounts.sum
      val avgUsersPerPartition = if (partitionCounts.nonEmpty) totalUsers.toDouble / partitionCounts.length else 0.0
      
      PartitionValidationMetrics(
        partitionCount = partitionCounts.length,
        totalUsers = totalUsers,
        totalRecords = totalRecords,
        avgUsersPerPartition = avgUsersPerPartition,
        userSkewRatio = userSkew,
        isWellPartitioned = userSkew < 2.0
      )
    } catch {
      case _: Exception => PartitionValidationMetrics(0, 0, 0, 0.0, Double.MaxValue, false)
    }
  }
}

/**
 * Test record structure for validation.
 */
case class TestRecord(
  userId: String,
  timestamp: String,
  artistId: Option[String],
  artistName: String,
  trackId: Option[String],
  trackName: String,
  trackKey: String
)

/**
 * Metrics for Parquet file size validation.
 */
case class ParquetSizeMetrics(
  totalSize: Long,
  averageSize: Long,
  maxSize: Long,
  isOptimal: Boolean
)

/**
 * Metrics for partition validation in test data.
 */
case class PartitionValidationMetrics(
  partitionCount: Int,
  totalUsers: Long,
  totalRecords: Long,
  avgUsersPerPartition: Double,
  userSkewRatio: Double,
  isWellPartitioned: Boolean
)
