package com.lastfm.sessions.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}

/**
 * Base test specification for Parquet-based testing with complete safety isolation.
 * 
 * CRITICAL SAFETY: Now extends BaseTestSpec for complete test isolation.
 * All Parquet operations are performed in data/test/ to prevent production contamination.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait ParquetTestSpec extends AnyFlatSpec with BaseTestSpec with Matchers {
  
  /**
   * Test Spark session optimized for Parquet operations.
   */
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("ParquetTestSpec")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.warehouse.dir", "data/test/silver/spark-warehouse")
    .getOrCreate()
  
  // Use test directories from BaseTestSpec (data/test/*)
  lazy val testBaseDir = getTestPath("silver")
  lazy val testDataDir = getTestPath("silver")
  lazy val testOutputDir = getTestPath("gold")
  
  private var createdTestDirs: List[String] = List.empty
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    createdTestDirs = List.empty
  }
  
  override def afterEach(): Unit = {
    ParquetTestUtils.cleanupParquetTestData(createdTestDirs: _*)
    super.afterEach()
  }
  
  def createParquetTestData(
    data: List[(String, String, String, String, String, String)],
    pathSuffix: String = ""
  ): String = {
    val timestamp = System.currentTimeMillis()
    val path = s"$testDataDir/test-data-$timestamp$pathSuffix"
    createdTestDirs = path :: createdTestDirs
    ParquetTestUtils.createParquetTestData(data, path)
  }
  
  def generateTestData(users: Int, tracksPerUser: Int): List[(String, String, String, String, String, String)] = {
    ParquetTestUtils.generateRealisticTestData(users, tracksPerUser)
  }
  
  def validateParquetOutput(path: String): Unit = {
    ParquetTestUtils.validateParquetStructure(path) should be(true)
    Files.exists(Paths.get(path)) should be(true)
    Files.isDirectory(Paths.get(path)) should be(true)
    val successFile = Paths.get(path, "_SUCCESS")
    Files.exists(successFile) should be(true)
  }
  
  def validateParquetRecordCount(path: String, expectedCount: Long): Unit = {
    val df = spark.read.parquet(path)
    df.count() should be(expectedCount)
  }
  
  def validateParquetUserCount(path: String, expectedUsers: Long): Unit = {
    val df = spark.read.parquet(path)
    val actualUsers = df.select("userId").distinct().count()
    actualUsers should be(expectedUsers)
  }
  
  def validateParquetPartitioning(path: String): Unit = {
    val df = spark.read.parquet(path)
    val partitionCount = df.rdd.partitions.length
    partitionCount should be >= 1
    partitionCount should be <= 50
  }
  
  def validateParquetFileSizes(path: String): Unit = {
    val directory = Paths.get(path)
    require(Files.exists(directory) && Files.isDirectory(directory), s"Directory does not exist: $path")
    val parquetFiles = Files.list(directory).filter(_.getFileName.toString.endsWith(".parquet")).toArray
    parquetFiles.length should be >= 1
  }
  
  def readParquetTestData(path: String): List[TestRecord] = {
    val df = spark.read.parquet(path)
    import spark.implicits._
    df.as[TestRecord].collect().toList
  }
  
  def createTempOutputDir(suffix: String = ""): String = {
    val timestamp = System.currentTimeMillis()
    val path = s"$testOutputDir/output-$timestamp$suffix"
    createdTestDirs = path :: createdTestDirs
    Files.createDirectories(Paths.get(path))
    path
  }
  
  def assertParquetContainsPatterns(path: String, expectedPatterns: String*): Unit = {
    val records = readParquetTestData(path)
    val content = records.map(_.toString).mkString(" ")
    expectedPatterns.foreach { pattern =>
      content should include(pattern)
    }
  }
  
  def assertTrackKeysGenerated(path: String): Unit = {
    val records = readParquetTestData(path)
    records.foreach { record =>
      if (record.trackId.isDefined && record.trackId.get.nonEmpty) {
        record.trackKey should be(record.trackId.get)
      } else {
        record.trackKey should be(s"${record.artistName} — ${record.trackName}")
      }
    }
  }
}

// TestRecord is defined in ParquetTestUtils.scala