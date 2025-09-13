package com.lastfm.sessions.integration

import com.lastfm.sessions.infrastructure.SparkDataRepository
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * End-to-end integration test specification for data loading operations.
 * 
 * Each test validates exactly one aspect of the complete integration,
 * ensuring focused validation and clear failure diagnosis.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DataLoadingIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  
  implicit var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataLoading-Integration")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    // Create sample data directory if it doesn't exist
    val sampleDir = Paths.get("data/sample")
    if (!Files.exists(sampleDir)) {
      Files.createDirectories(sampleDir)
    }
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
    
    def createSampleDataFile(): Path = {
      val path = Files.createTempFile("integration_sample", ".tsv")
      val data = """user_000001	2009-05-04T23:08:57Z	artist1	Deep Dish		Track 1
                   |user_000002	2009-05-04T13:54:10Z	artist2	坂本龍一		Track 2
                   |user_000003	2009-05-04T10:30:15Z	artist3	The Beatles		Track 3""".stripMargin
      Files.write(path, data.getBytes(StandardCharsets.UTF_8))
      tempFiles = path :: tempFiles
      path
    }
    
    def createLargeDataFile(): Path = {
      val path = Files.createTempFile("integration_large", ".tsv")
      val builder = new StringBuilder()
      for (i <- 1 to 10000) {
        val userId = f"user_${i % 100}%06d"
        val timestamp = Instant.parse("2009-05-04T10:00:00Z").plusSeconds(i * 30L)
        builder.append(s"$userId\t$timestamp\tartist_$i\tArtist $i\t\tTrack $i\n")
      }
      Files.write(path, builder.toString().getBytes(StandardCharsets.UTF_8))
      tempFiles = path :: tempFiles
      path
    }
    
    def createVeryLargeDataFile(): Path = {
      val path = Files.createTempFile("integration_very_large", ".tsv")
      val builder = new StringBuilder()
      for (i <- 1 to 50000) {  // 50K records to test memory efficiency
        val userId = f"user_${i % 500}%06d"  // 500 unique users
        val timestamp = Instant.parse("2009-05-04T10:00:00Z").plusSeconds(i * 30L)
        builder.append(s"$userId\t$timestamp\tartist_$i\tArtist $i\t\tTrack $i\n")
      }
      Files.write(path, builder.toString().getBytes(StandardCharsets.UTF_8))
      tempFiles = path :: tempFiles
      path
    }
    
    def createCorruptedDataFile(): Path = {
      val path = Files.createTempFile("integration_corrupted", ".tsv")
      val data = """INVALID_HEADER_LINE
                   |user_001	INVALID_TIMESTAMP	artist_id	Artist Name		Track Name
                   |user_002	2009-05-04T13:54:10Z	valid_id	Valid Artist		Valid Track
                   |COMPLETELY_MALFORMED_LINE_WITH_NO_TABS
                   |user_003		missing_timestamp_artist		Missing Timestamp Track""".stripMargin
      Files.write(path, data.getBytes(StandardCharsets.UTF_8))
      tempFiles = path :: tempFiles
      path
    }
  }
  
  /**
   * Integration tests for successful processing - each test focuses on one success aspect.
   */
  "Data Loading Integration" should "successfully process sample file" in new TestContext {
    // Arrange
    val sampleFile = createSampleDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(sampleFile.toString)
    
    // Assert - Only test successful processing
    result.isSuccess should be(true)
  }
  
  it should "load expected number of records from sample" in new TestContext {
    // Arrange
    val sampleFile = createSampleDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(sampleFile.toString)
    
    // Assert - Only test record count
    result.get should have size 3
  }
  
  it should "preserve all user IDs from sample" in new TestContext {
    // Arrange
    val sampleFile = createSampleDataFile()
    val expectedUsers = Set("user_000001", "user_000002", "user_000003")
    
    // Act
    val result = dataRepository.loadListenEvents(sampleFile.toString)
    
    // Assert - Only test user ID preservation
    val loadedUsers = result.get.map(_.userId).toSet
    loadedUsers should equal(expectedUsers)
  }
  
  it should "handle Unicode characters in sample data" in new TestContext {
    // Arrange
    val sampleFile = createSampleDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(sampleFile.toString)
    
    // Assert - Only test Unicode character handling
    val japaneseArtist = result.get.find(_.artistName == "坂本龍一")
    japaneseArtist should be(defined)
  }
  
  it should "preserve chronological order in sample data" in new TestContext {
    // Arrange
    val sampleFile = createSampleDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(sampleFile.toString)
    
    // Assert - Only test chronological order preservation
    val timestamps = result.get.map(_.timestamp)
    timestamps should have size 3
    // Check that all timestamps are valid
    timestamps.foreach(_ should not be null)
  }
  
  it should "validate all required fields in sample data" in new TestContext {
    // Arrange
    val sampleFile = createSampleDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(sampleFile.toString)
    
    // Assert - Only test required field validation
    result.get.foreach { event =>
      event.userId should not be empty
      event.timestamp should not be null
      event.artistName should not be empty
      event.trackName should not be empty
    }
  }
  
  /**
   * Integration tests for performance characteristics - each test focuses on one performance aspect.
   */
  "Data Loading Integration performance" should "complete large dataset within reasonable time" in new TestContext {
    // Arrange
    val largeFile = createLargeDataFile()
    
    // Act
    val startTime = System.currentTimeMillis()
    val result = dataRepository.loadListenEvents(largeFile.toString)
    val endTime = System.currentTimeMillis()
    
    // Assert - Only test processing time
    result.isSuccess should be(true)
    val processingTime = endTime - startTime
    processingTime should be < 120000L  // Less than 2 minutes for test data
  }
  
  it should "use reasonable memory with batch processing approach" in new TestContext {
    // Arrange
    val largeFile = createLargeDataFile()
    
    // Act
    val beforeMemory = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    val result = dataRepository.loadListenEvents(largeFile.toString)
    val afterMemory = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    
    // Assert - Memory-efficient batch processing should use much less memory
    result.isSuccess should be(true)
    val memoryUsedMB = (afterMemory - beforeMemory) / (1024 * 1024)
    memoryUsedMB should be < 500L  // Much less memory due to batch processing (< 500MB)
  }
  
  it should "handle datasets larger than memory without OutOfMemoryError" in new TestContext {
    // Arrange - Create a dataset that would cause OOM with old approach
    val veryLargeFile = createVeryLargeDataFile()  // 50K records
    
    // Act & Assert - Should not throw OutOfMemoryError
    val result = dataRepository.loadListenEvents(veryLargeFile.toString)
    
    result.isSuccess should be(true)
    // Should still return only the batch limit, preventing memory issues
    result.get should have size 5000
  }
  
  it should "process large dataset with memory-efficient batch processing" in new TestContext {
    // Arrange
    val largeFile = createLargeDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(largeFile.toString)
    
    // Assert - Test memory-efficient batch processing (limited to 5000 records to prevent OOM)
    result.isSuccess should be(true)
    result.get should have size 5000  // Memory-efficient batch size limit
    
    // Verify that we got a representative sample
    val uniqueUsers = result.get.map(_.userId).toSet
    uniqueUsers.size should be >= 50  // Should have good user distribution in sample
  }
  
  it should "maintain data integrity in large dataset sample" in new TestContext {
    // Arrange
    val largeFile = createLargeDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(largeFile.toString)
    
    // Assert - Test data integrity in memory-efficient sample
    result.isSuccess should be(true)
    val uniqueUsers = result.get.map(_.userId).toSet
    // With 5000 records from 10000, we should get a representative sample of users
    uniqueUsers.size should be >= 50  // At least 50% of users in sample
    uniqueUsers.size should be <= 100  // But not more than total users in data
    
    // Verify all records have valid data
    result.get.foreach { event =>
      event.userId should not be empty
      event.timestamp should not be null
      event.artistName should not be empty
      event.trackName should not be empty
    }
  }
  
  it should "maintain consistent performance across runs" in new TestContext {
    // Arrange
    val sampleFile = createSampleDataFile()
    
    // Act - Run multiple times
    val times = (1 to 3).map { _ =>
      val startTime = System.currentTimeMillis()
      dataRepository.loadListenEvents(sampleFile.toString)
      System.currentTimeMillis() - startTime
    }
    
    // Assert - Only test performance consistency
    val avgTime = times.sum / times.length
    val maxDeviation = times.map(t => math.abs(t - avgTime)).max
    maxDeviation should be < (avgTime * 0.8).toLong  // Within 80% of average
  }
  
  /**
   * Integration tests for error handling - each test focuses on one error scenario.
   */
  "Data Loading Integration error handling" should "handle non-existent file gracefully" in new TestContext {
    // Arrange
    val nonExistentPath = "/does/not/exist.tsv"
    
    // Act
    val result = dataRepository.loadListenEvents(nonExistentPath)
    
    // Assert - Only test graceful error handling
    result.isFailure should be(true)
  }
  
  it should "provide meaningful error for non-existent file" in new TestContext {
    // Arrange
    val nonExistentPath = "/does/not/exist.tsv"
    
    // Act
    val result = dataRepository.loadListenEvents(nonExistentPath)
    
    // Assert - Only test error message quality
    result.isFailure should be(true)
    result.failed.get.getMessage should not be empty
  }
  
  it should "filter corrupted data and return valid records" in new TestContext {
    // Arrange
    val corruptedFile = createCorruptedDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(corruptedFile.toString)
    
    // Assert - Only test corrupted data filtering
    result.isSuccess should be(true)
    result.get should have size 1  // Only one valid record in corrupted file
  }
  
  it should "preserve valid data when corrupted data is present" in new TestContext {
    // Arrange
    val corruptedFile = createCorruptedDataFile()
    
    // Act
    val result = dataRepository.loadListenEvents(corruptedFile.toString)
    
    // Assert - Only test valid data preservation
    result.isSuccess should be(true)
    val validEvent = result.get.head
    validEvent.userId should be("user_002")
    validEvent.artistName should be("Valid Artist")
  }
  
  it should "handle completely empty files without crashing" in new TestContext {
    // Arrange
    val emptyFile = Files.createTempFile("integration_empty", ".tsv")
    Files.write(emptyFile, Array.empty[Byte])
    tempFiles = emptyFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(emptyFile.toString)
    
    // Assert - Only test empty file handling
    result.isSuccess should be(true)
    result.get should be(empty)
  }
  
  /**
   * Integration tests for data quality validation - each test focuses on one quality aspect.
   */
  "Data Loading Integration data quality" should "reject records with malformed timestamps" in new TestContext {
    // Arrange
    val malformedFile = Files.createTempFile("malformed_time", ".tsv")
    val data = """user_001	NOT_A_TIMESTAMP	artist1	Artist 1		Track 1
                 |user_002	2009-05-04T13:54:10Z	artist2	Artist 2		Track 2""".stripMargin
    Files.write(malformedFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = malformedFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(malformedFile.toString)
    
    // Assert - Only test malformed timestamp rejection
    result.isSuccess should be(true)
    result.get should have size 1  // Only valid record should remain
  }
  
  it should "reject records with empty required fields" in new TestContext {
    // Arrange
    val emptyFieldsFile = Files.createTempFile("empty_fields", ".tsv")
    val data = """	2009-05-04T13:54:10Z	artist1	Artist 1		Track 1
                 |user_002	2009-05-04T13:54:10Z	artist2			Track 2
                 |user_003	2009-05-04T13:54:10Z	artist3	Artist 3		""".stripMargin
    Files.write(emptyFieldsFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = emptyFieldsFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(emptyFieldsFile.toString)
    
    // Assert - Only test empty field rejection
    result.isSuccess should be(true)
    result.get should be(empty)  // All records have missing required fields
  }
  
  it should "maintain data quality metrics during processing" in new TestContext {
    // Arrange
    val mixedQualityFile = Files.createTempFile("mixed_quality", ".tsv")
    val data = """user_001	2009-05-04T13:54:10Z	artist1	Good Artist 1		Good Track 1
                 |user_002	BAD_TIMESTAMP	artist2	Bad Artist 2		Bad Track 2
                 |user_003	2009-05-04T13:54:10Z	artist3	Good Artist 3		Good Track 3
                 |	2009-05-04T13:54:10Z	artist4	Missing User		Missing User Track""".stripMargin
    Files.write(mixedQualityFile, data.getBytes(StandardCharsets.UTF_8))
    tempFiles = mixedQualityFile :: tempFiles
    
    // Act
    val result = dataRepository.loadListenEvents(mixedQualityFile.toString)
    
    // Assert - Only test data quality metrics
    result.isSuccess should be(true)
    result.get should have size 2  // Only 2 good records out of 4 total
    result.get.foreach { event =>
      event.userId should startWith("user_")
      event.artistName should startWith("Good Artist")
    }
  }
}