package com.lastfm.sessions.testutil

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Base test specification for Parquet-based testing infrastructure.
 * 
 * Provides common functionality for tests that work with Parquet data format:
 * - Spark session management optimized for testing
 * - Parquet test data creation and cleanup
 * - Common assertion methods for Parquet validation
 * - Memory-efficient test data handling
 * 
 * Key Features:
 * - Automatic cleanup of test data between tests
 * - Optimized Spark configuration for test performance
 * - Parquet-specific validation methods
 * - Support for realistic test data generation
 * - Format-agnostic data creation utilities
 * 
 * Design Principles:
 * - Test Isolation: Each test gets clean state
 * - Performance: Efficient Spark operations for tests
 * - Reusability: Common patterns in base trait
 * - Maintainability: Centralized test infrastructure
 * 
 * Usage:
 * ```scala
 * class MyParquetTest extends ParquetTestSpec {
 *   "My pipeline" should "process Parquet data correctly" in {
 *     val testData = generateTestData(users = 10, tracksPerUser = 5)
 *     val parquetPath = createParquetTestData(testData)
 *     
 *     // Test logic here
 *     
 *     validateParquetOutput(parquetPath)
 *   }
 * }
 * ```
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait ParquetTestSpec extends AnyFlatSpec with Matchers with TestEnvironment {
  
  /**
   * Test Spark session optimized for Parquet operations.
   */
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("ParquetTestSpec")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4") // Small for tests
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false") // Disable UI for tests
    .config("spark.sql.adaptive.enabled", "false") // Predictable behavior for tests
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    // Parquet-specific test optimizations
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.filterPushdown", "true")
    // Use test root directory for Spark warehouse
    .config("spark.sql.warehouse.dir", s"$testRootDir/spark-warehouse")
    .getOrCreate()
  
  /**
   * Use test directories from TestEnvironment.
   */
  val testBaseDir = testRootDir
  val testDataDir = testSilverDir
  val testOutputDir = testGoldDir
  
  /**
   * Tracks created test directories for cleanup.
   */
  private var createdTestDirs: List[String] = List.empty
  
  override def beforeEach(): Unit = {
    super.beforeEach() // Calls TestEnvironment's setup
    createdTestDirs = List.empty
  }
  
  override def afterEach(): Unit = {
    // Clean Parquet-specific test data
    ParquetTestUtils.cleanupParquetTestData(createdTestDirs: _*)
    super.afterEach() // Calls TestEnvironment's cleanup
  }
  
  /**
   * Creates Parquet test data from structured input.
   * 
   * @param data Test data as list of tuples
   * @param pathSuffix Optional path suffix for multiple test datasets
   * @return Path to created Parquet data
   */
  def createParquetTestData(
    data: List[(String, String, String, String, String, String)],
    pathSuffix: String = ""
  ): String = {
    val timestamp = System.currentTimeMillis()
    val path = s"$testDataDir/test-data-$timestamp$pathSuffix"
    createdTestDirs = path :: createdTestDirs
    ParquetTestUtils.createParquetTestData(data, path)
  }
  
  /**
   * Generates realistic test data with proper distributions.
   * 
   * @param users Number of users to generate
   * @param tracksPerUser Average tracks per user
   * @return Generated test data
   */
  def generateTestData(users: Int, tracksPerUser: Int): List[(String, String, String, String, String, String)] = {
    ParquetTestUtils.generateRealisticTestData(users, tracksPerUser)
  }
  
  /**
   * Validates Parquet output structure and content.
   * 
   * @param path Path to Parquet directory
   */
  def validateParquetOutput(path: String): Unit = {
    ParquetTestUtils.validateParquetStructure(path) should be(true)
    
    // Additional validation
    Files.exists(Paths.get(path)) should be(true)
    Files.isDirectory(Paths.get(path)) should be(true)
    
    // Check for _SUCCESS file (Spark completion marker)
    val successFile = Paths.get(path, "_SUCCESS")
    Files.exists(successFile) should be(true)
  }
  
  /**
   * Validates Parquet data contains expected number of records.
   * 
   * @param path Path to Parquet directory
   * @param expectedCount Expected record count
   */
  def validateParquetRecordCount(path: String, expectedCount: Long): Unit = {
    val df = spark.read.parquet(path)
    df.count() should be(expectedCount)
  }
  
  /**
   * Validates Parquet data contains expected users.
   * 
   * @param path Path to Parquet directory
   * @param expectedUsers Expected number of unique users
   */
  def validateParquetUserCount(path: String, expectedUsers: Long): Unit = {
    val df = spark.read.parquet(path)
    val userCount = df.select("userId").distinct().count()
    userCount should be(expectedUsers)
  }
  
  /**
   * Validates Parquet partitioning meets performance criteria.
   * 
   * @param path Path to partitioned Parquet data
   */
  def validateParquetPartitioning(path: String): Unit = {
    val metrics = ParquetTestUtils.validatePartitioning(path)
    
    // Basic partitioning validation
    metrics.partitionCount should be > 0
    metrics.totalRecords should be > 0L
    
    // Skew should be reasonable for test data
    metrics.userSkewRatio should be < 5.0 // Allow more skew in test data
    
    // Should have some users in each partition (for non-empty test data)
    if (metrics.totalUsers > 0) {
      metrics.avgUsersPerPartition should be > 0.0
    }
  }
  
  /**
   * Validates Parquet file sizes are appropriate for test data.
   * 
   * @param path Path to Parquet directory
   */
  def validateParquetFileSizes(path: String): Unit = {
    val metrics = ParquetTestUtils.validateParquetFileSizes(path)
    
    // Files should exist and have reasonable sizes for test data
    metrics.totalSize should be > 0L
    metrics.averageSize should be > 0L
    
    // Files shouldn't be too large for test data
    metrics.maxSize should be < (50 * 1024 * 1024L) // 50MB max for test files
  }
  
  /**
   * Reads Parquet test data for validation.
   * 
   * @param path Path to Parquet directory
   * @return Test records for validation
   */
  def readParquetTestData(path: String): List[TestRecord] = {
    ParquetTestUtils.readParquetRecords(path)
  }
  
  /**
   * Creates temporary output directory for test.
   * 
   * @param suffix Directory suffix for identification
   * @return Path to created directory
   */
  def createTempOutputDir(suffix: String = ""): String = {
    val timestamp = System.currentTimeMillis()
    val path = s"$testOutputDir/output-$timestamp$suffix"
    createdTestDirs = path :: createdTestDirs
    Files.createDirectories(Paths.get(path))
    path
  }
  
  /**
   * Asserts that Parquet data matches expected content patterns.
   * 
   * @param path Path to Parquet directory
   * @param expectedPatterns Content patterns to validate
   */
  def assertParquetContainsPatterns(path: String, expectedPatterns: String*): Unit = {
    val records = readParquetTestData(path)
    val content = records.map(_.toString).mkString(" ")
    
    expectedPatterns.foreach { pattern =>
      content should include(pattern)
    }
  }
  
  /**
   * Asserts that Parquet data has proper track key generation.
   * 
   * @param path Path to Parquet directory
   */
  def assertTrackKeysGenerated(path: String): Unit = {
    val records = readParquetTestData(path)
    
    records.foreach { record =>
      // Track key should be either trackId or "artistName — trackName"
      if (record.trackId.isDefined && record.trackId.get.nonEmpty) {
        record.trackKey should be(record.trackId.get)
      } else {
        record.trackKey should be(s"${record.artistName} — ${record.trackName}")
      }
    }
  }
  
  // Cleanup methods removed - now handled by TestEnvironment trait
  // TestEnvironment provides createTestDirectories() and cleanup of all test artifacts
}
