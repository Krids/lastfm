package com.lastfm.sessions.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalamock.scalatest.MockFactory
import java.time.Instant
import java.io.FileNotFoundException
import java.nio.file.AccessDeniedException
import scala.util.{Try, Success, Failure}

/**
 * Test specification for the DataRepositoryPort interface.
 * 
 * Each test validates exactly one contract or behavior of the data loading port,
 * ensuring focused validation and clear failure diagnosis.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DataRepositoryPortSpec extends AnyFlatSpec with Matchers with MockFactory {
  
  trait TestContext {
    val mockDataRepository: DataRepositoryPort = mock[DataRepositoryPort]
  }
  
  /**
   * Tests for successful loading scenarios - each test focuses on one success aspect.
   */
  "DataRepositoryPort successful loading" should "return Success when file exists" in new TestContext {
    // Arrange
    val validPath = "data/valid-file.tsv"
    val sampleEvent = ListenEvent("user_001", Instant.now(), None, "Artist", None, "Track")
    (mockDataRepository.loadListenEvents _)
      .expects(validPath)
      .returning(Success(List(sampleEvent)))
    
    // Act
    val result = mockDataRepository.loadListenEvents(validPath)
    
    // Assert - Only test that result is Success
    result.isSuccess should be(true)
  }
  
  it should "return non-empty list when data exists" in new TestContext {
    // Arrange
    val validPath = "data/with-data.tsv"
    val sampleEvents = List(
      ListenEvent("user_001", Instant.now(), None, "Artist1", None, "Track1"),
      ListenEvent("user_002", Instant.now(), None, "Artist2", None, "Track2")
    )
    (mockDataRepository.loadListenEvents _)
      .expects(validPath)
      .returning(Success(sampleEvents))
    
    // Act
    val result = mockDataRepository.loadListenEvents(validPath)
    
    // Assert - Only test that list is non-empty
    result.get should not be empty
  }
  
  it should "return correct number of events" in new TestContext {
    // Arrange
    val validPath = "data/counted-events.tsv"
    val expectedCount = 3
    val sampleEvents = List.fill(expectedCount)(
      ListenEvent("user_001", Instant.now(), None, "Artist", None, "Track")
    )
    (mockDataRepository.loadListenEvents _)
      .expects(validPath)
      .returning(Success(sampleEvents))
    
    // Act
    val result = mockDataRepository.loadListenEvents(validPath)
    
    // Assert - Only test event count
    result.get should have size expectedCount
  }
  
  it should "return empty list when file is empty" in new TestContext {
    // Arrange
    val emptyPath = "data/empty-file.tsv"
    (mockDataRepository.loadListenEvents _)
      .expects(emptyPath)
      .returning(Success(List.empty))
    
    // Act
    val result = mockDataRepository.loadListenEvents(emptyPath)
    
    // Assert - Only test empty list handling
    result.get should be(empty)
  }
  
  /**
   * Tests for error scenarios - each test focuses on one specific error type.
   */
  "DataRepositoryPort error handling" should "return Failure for FileNotFoundException" in new TestContext {
    // Arrange
    val nonExistentPath = "data/does-not-exist.tsv"
    val exception = new FileNotFoundException("File not found")
    (mockDataRepository.loadListenEvents _)
      .expects(nonExistentPath)
      .returning(Failure(exception))
    
    // Act
    val result = mockDataRepository.loadListenEvents(nonExistentPath)
    
    // Assert - Only test FileNotFoundException handling
    result.isFailure should be(true)
    result.failed.get shouldBe a[FileNotFoundException]
  }
  
  it should "return Failure for AccessDeniedException" in new TestContext {
    // Arrange
    val restrictedPath = "/root/restricted.tsv"
    val exception = new AccessDeniedException("Permission denied")
    (mockDataRepository.loadListenEvents _)
      .expects(restrictedPath)
      .returning(Failure(exception))
    
    // Act
    val result = mockDataRepository.loadListenEvents(restrictedPath)
    
    // Assert - Only test AccessDeniedException handling
    result.isFailure should be(true)
    result.failed.get shouldBe a[AccessDeniedException]
  }
  
  it should "return Failure for data corruption" in new TestContext {
    // Arrange
    val corruptedPath = "data/corrupted.tsv"
    val exception = new IllegalArgumentException("Malformed data")
    (mockDataRepository.loadListenEvents _)
      .expects(corruptedPath)
      .returning(Failure(exception))
    
    // Act
    val result = mockDataRepository.loadListenEvents(corruptedPath)
    
    // Assert - Only test data corruption handling
    result.isFailure should be(true)
    result.failed.get shouldBe an[IllegalArgumentException]
  }
  
  it should "preserve original exception message" in new TestContext {
    // Arrange
    val testPath = "data/test.tsv"
    val originalMessage = "Specific error details"
    val exception = new RuntimeException(originalMessage)
    (mockDataRepository.loadListenEvents _)
      .expects(testPath)
      .returning(Failure(exception))
    
    // Act
    val result = mockDataRepository.loadListenEvents(testPath)
    
    // Assert - Only test message preservation
    result.failed.get.getMessage should be(originalMessage)
  }
  
  /**
   * Tests for data quality validation - each test focuses on one quality aspect.
   */
  "DataRepositoryPort data quality" should "handle records with all required fields" in new TestContext {
    // Arrange
    val validPath = "data/complete-records.tsv"
    val completeEvent = ListenEvent(
      userId = "user_000001",
      timestamp = Instant.parse("2009-05-04T23:08:57Z"),
      artistId = Some("artist_id"),
      artistName = "Deep Dish",
      trackId = Some("track_id"),
      trackName = "Track Name"
    )
    (mockDataRepository.loadListenEvents _)
      .expects(validPath)
      .returning(Success(List(completeEvent)))
    
    // Act
    val result = mockDataRepository.loadListenEvents(validPath)
    
    // Assert - Only test complete record handling
    val event = result.get.head
    event.artistId should be(defined)
    event.trackId should be(defined)
  }
  
  it should "handle records with missing optional fields" in new TestContext {
    // Arrange
    val validPath = "data/minimal-records.tsv"
    val minimalEvent = ListenEvent.minimal(
      userId = "user_000001",
      timestamp = Instant.now(),
      artistName = "Artist",
      trackName = "Track"
    )
    (mockDataRepository.loadListenEvents _)
      .expects(validPath)
      .returning(Success(List(minimalEvent)))
    
    // Act
    val result = mockDataRepository.loadListenEvents(validPath)
    
    // Assert - Only test minimal record handling
    val event = result.get.head
    event.artistId should be(None)
    event.trackId should be(None)
  }
  
  it should "validate userId format" in new TestContext {
    // Arrange
    val validPath = "data/validated-userids.tsv"
    val validUserEvent = ListenEvent("user_000001", Instant.now(), None, "Artist", None, "Track")
    (mockDataRepository.loadListenEvents _)
      .expects(validPath)
      .returning(Success(List(validUserEvent)))
    
    // Act
    val result = mockDataRepository.loadListenEvents(validPath)
    
    // Assert - Only test userId validation
    result.get.head.userId should startWith("user_")
  }
  
  it should "validate timestamp format" in new TestContext {
    // Arrange
    val validPath = "data/validated-timestamps.tsv"
    val validTimestamp = Instant.parse("2009-05-04T23:08:57Z")
    val timestampEvent = ListenEvent("user_001", validTimestamp, None, "Artist", None, "Track")
    (mockDataRepository.loadListenEvents _)
      .expects(validPath)
      .returning(Success(List(timestampEvent)))
    
    // Act
    val result = mockDataRepository.loadListenEvents(validPath)
    
    // Assert - Only test timestamp validation
    val event = result.get.head
    event.timestamp should be(validTimestamp)
    event.timestamp should not be null
  }
}

