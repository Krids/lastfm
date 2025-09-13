package com.lastfm.sessions.infrastructure

import com.lastfm.sessions.domain.ListenEvent
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * Test specification for the Spark-based data repository implementation.
 * 
 * Each test focuses on one specific aspect of Spark integration,
 * ensuring clear validation and focused failure diagnosis.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class SparkDataRepositorySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  
  implicit var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SparkDataRepository-Tests")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }
  
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }
  
  // Clean up any temporary files after each test
  var tempFiles: List[Path] = List.empty
  
  override def afterEach(): Unit = {
    tempFiles.foreach(Files.deleteIfExists)
    tempFiles = List.empty
  }
  
  trait TestContext {
    val dataRepository = new SparkDataRepository()
    
    def createValidTestFile(): Path = {
      val path = Files.createTempFile("valid_test", ".tsv")
      val data = "user_000001\t2009-05-04T23:08:57Z\tartist_id\tDeep Dish\t\tTrack Name"
      Files.write(path, data.getBytes(StandardCharsets.UTF_8))
      tempFiles = path :: tempFiles
      path
    }
    
    def createMalformedTimestampFile(): Path = {
      val path = Files.createTempFile("malformed_timestamp", ".tsv")
      val data = "user_001\tINVALID_TIMESTAMP\tartist_id\tArtist\t\tTrack"
      Files.write(path, data.getBytes(StandardCharsets.UTF_8))
      tempFiles = path :: tempFiles
      path
    }
    
    def createMissingUserIdFile(): Path = {
      val path = Files.createTempFile("missing_user", ".tsv")
      val data = "\t2009-05-04T23:08:57Z\tartist_id\tArtist\t\tTrack"
      Files.write(path, data.getBytes(StandardCharsets.UTF_8))
      tempFiles = path :: tempFiles
      path
    }
    
    def createEmptyFile(): Path = {
      val path = Files.createTempFile("empty", ".tsv")
      Files.write(path, Array.empty[Byte])
      tempFiles = path :: tempFiles
      path
    }
  }
  
  /**
   * Tests for successful file loading - each test validates one loading aspect.
   */
  "SparkDataRepository file loading" should "successfully load valid TSV file" in new TestContext {
    // Arrange
    val testFile = createValidTestFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test successful loading
    result.isSuccess should be(true)
  }
  
  it should "return correct number of valid records" in new TestContext {
    // Arrange
    val testFile = createValidTestFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test record count
    result.get should have size 1
  }
  
  it should "parse userId correctly" in new TestContext {
    // Arrange
    val testFile = createValidTestFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test userId parsing
    result.get.head.userId should be("user_000001")
  }
  
  it should "parse timestamp correctly" in new TestContext {
    // Arrange
    val expectedTimestamp = Instant.parse("2009-05-04T23:08:57Z")
    val testFile = createValidTestFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test timestamp parsing
    result.get.head.timestamp should be(expectedTimestamp)
  }
  
  it should "parse artistName correctly" in new TestContext {
    // Arrange
    val testFile = createValidTestFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test artistName parsing
    result.get.head.artistName should be("Deep Dish")
  }
  
  it should "parse trackName correctly" in new TestContext {
    // Arrange
    val testFile = createValidTestFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test trackName parsing
    result.get.head.trackName should be("Track Name")
  }
  
  it should "handle optional artistId when present" in new TestContext {
    // Arrange
    val testFile = createValidTestFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test optional artistId handling
    result.get.head.artistId should be(Some("artist_id"))
  }
  
  it should "handle optional trackId when empty" in new TestContext {
    // Arrange
    val testFile = createValidTestFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test optional trackId handling (empty in test data)
    result.get.head.trackId should be(None)
  }
  
  /**
   * Tests for malformed data handling - each test focuses on one validation rule.
   */
  "SparkDataRepository validation" should "filter records with invalid timestamps" in new TestContext {
    // Arrange
    val testFile = createMalformedTimestampFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test invalid timestamp filtering
    result.get should be(empty)
  }
  
  it should "filter records with missing userId" in new TestContext {
    // Arrange
    val testFile = createMissingUserIdFile()
    
    // Act
    val result = dataRepository.loadListenEvents(testFile.toString)
    
    // Assert - Only test missing userId filtering
    result.get should be(empty)
  }
  
  it should "handle empty files without error" in new TestContext {
    // Arrange
    val emptyFile = createEmptyFile()
    
    // Act
    val result = dataRepository.loadListenEvents(emptyFile.toString)
    
    // Assert - Only test empty file handling
    result.isSuccess should be(true)
  }
  
  it should "return empty list for empty files" in new TestContext {
    // Arrange
    val emptyFile = createEmptyFile()
    
    // Act
    val result = dataRepository.loadListenEvents(emptyFile.toString)
    
    // Assert - Only test empty list return
    result.get should be(empty)
  }
  
  it should "filter records with empty artistName" in new TestContext {
    // Arrange
    val path = Files.createTempFile("empty_artist", ".tsv")
    val data = "user_001\t2009-05-04T23:08:57Z\tartist_id\t\t\tTrack"
    Files.write(path, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = path :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(path.toString)
    
    // Assert - Only test empty artistName filtering
    result.get should be(empty)
  }
  
  it should "filter records with empty trackName" in new TestContext {
    // Arrange
    val path = Files.createTempFile("empty_track", ".tsv")
    val data = "user_001\t2009-05-04T23:08:57Z\tartist_id\tArtist\t\t"
    Files.write(path, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = path :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(path.toString)
    
    // Assert - Only test empty trackName filtering
    result.get should be(empty)
  }
  
  /**
   * Tests for Unicode character handling - each test focuses on one character type.
   */
  "SparkDataRepository Unicode support" should "preserve Japanese characters in artistName" in new TestContext {
    // Arrange
    val japaneseArtist = "坂本龍一"
    val unicodeFile = Files.createTempFile("japanese", ".tsv")
    val data = s"user_001\t2009-05-04T23:08:57Z\tartist_id\t$japaneseArtist\t\tTrack"
    Files.write(unicodeFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = unicodeFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(unicodeFile.toString)
    
    // Assert - Only test Japanese character preservation
    result.get.head.artistName should be(japaneseArtist)
  }
  
  it should "preserve accented characters in trackName" in new TestContext {
    // Arrange
    val accentedTrack = "Café del Mar"
    val unicodeFile = Files.createTempFile("accented", ".tsv")
    val data = s"user_001\t2009-05-04T23:08:57Z\tartist_id\tArtist\t\t$accentedTrack"
    Files.write(unicodeFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = unicodeFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(unicodeFile.toString)
    
    // Assert - Only test accented character preservation
    result.get.head.trackName should be(accentedTrack)
  }
  
  it should "preserve special characters in artistName" in new TestContext {
    // Arrange
    val specialArtist = "Sigur Rós"
    val unicodeFile = Files.createTempFile("special", ".tsv")
    val data = s"user_001\t2009-05-04T23:08:57Z\tartist_id\t$specialArtist\t\tTrack"
    Files.write(unicodeFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = unicodeFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(unicodeFile.toString)
    
    // Assert - Only test special character preservation
    result.get.head.artistName should be(specialArtist)
  }
  
  /**
   * Tests for multiple records handling - each test focuses on one aspect.
   */
  "SparkDataRepository multiple records" should "load multiple valid records" in new TestContext {
    // Arrange
    val multiFile = Files.createTempFile("multi", ".tsv")
    val data = """user_001	2009-05-04T23:08:57Z	artist1	Artist One		Track One
                 |user_002	2009-05-04T23:09:57Z	artist2	Artist Two		Track Two""".stripMargin
    Files.write(multiFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = multiFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(multiFile.toString)
    
    // Assert - Only test multiple record count
    result.get should have size 2
  }
  
  it should "preserve order of valid records" in new TestContext {
    // Arrange
    val multiFile = Files.createTempFile("ordered", ".tsv")
    val data = """user_001	2009-05-04T23:08:57Z	artist1	First Artist		Track One
                 |user_002	2009-05-04T23:09:57Z	artist2	Second Artist		Track Two""".stripMargin
    Files.write(multiFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = multiFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(multiFile.toString)
    
    // Assert - Only test record ordering
    result.get.head.artistName should be("First Artist")
    result.get(1).artistName should be("Second Artist")
  }
  
  it should "filter invalid records while preserving valid ones" in new TestContext {
    // Arrange
    val mixedFile = Files.createTempFile("mixed", ".tsv")
    val data = """user_001	INVALID_TIME	artist1	Artist One		Track One
                 |user_002	2009-05-04T23:09:57Z	artist2	Artist Two		Track Two
                 |	2009-05-04T23:10:57Z	artist3	Artist Three		Track Three""".stripMargin
    Files.write(mixedFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = mixedFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(mixedFile.toString)
    
    // Assert - Only test selective filtering (only valid record should remain)
    result.get should have size 1
    result.get.head.artistName should be("Artist Two")
  }
  
  /**
   * Tests for error conditions - each test focuses on one specific error scenario.
   */
  "SparkDataRepository error handling" should "fail gracefully for non-existent files" in new TestContext {
    // Arrange
    val nonExistentPath = "/path/that/does/not/exist.tsv"
    
    // Act
    val result = dataRepository.loadListenEvents(nonExistentPath)
    
    // Assert - Only test non-existent file handling
    result.isFailure should be(true)
  }
  
  it should "provide meaningful error for non-existent files" in new TestContext {
    // Arrange
    val nonExistentPath = "/path/that/does/not/exist.tsv"
    
    // Act
    val result = dataRepository.loadListenEvents(nonExistentPath)
    
    // Assert - Only test error message meaningfulness
    result.failed.get.getMessage should not be empty
  }
}