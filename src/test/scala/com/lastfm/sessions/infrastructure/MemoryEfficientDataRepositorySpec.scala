package com.lastfm.sessions.infrastructure

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import com.lastfm.sessions.domain.ListenEvent
import java.time.Instant
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.util.Success

/**
 * Test specification for memory-efficient data processing in SparkDataRepository.
 * 
 * These tests verify that the data repository can handle large static TSV datasets without
 * running into OutOfMemoryErrors by using batch processing approaches.
 */
class MemoryEfficientDataRepositorySpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("MemoryEfficientDataRepositorySpec")
    .master("local[2]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .getOrCreate()

  val repository = new SparkDataRepository()

  "SparkDataRepository with batch processing" should "process large static datasets without memory issues" in {
    // Create a test dataset with realistic size (10K records)
    val testDataPath = createTestDataset(10000)
    
    try {
      val result = repository.loadListenEventsBatched(testDataPath)
      
      result shouldBe a[Success[_]]
      result.get.size should be >= 4500 // Limited by maxReturnSize but should get good sample
      
    } finally {
      Files.deleteIfExists(Paths.get(testDataPath))
    }
  }

  it should "calculate quality metrics without loading all events into memory" in {
    val testDataPath = createTestDataset(5000)
    
    try {
      val result = repository.loadWithDataQualityBatched(testDataPath)
      
      result shouldBe a[Success[_]]
      val (sampleEvents, metrics) = result.get
      
      // Should return a sample, not all events
      sampleEvents.size should be <= 1000 // Sample size limit
      metrics.totalRecords should be >= 5000L
      metrics.validRecords should be <= metrics.totalRecords
      
    } finally {
      Files.deleteIfExists(Paths.get(testDataPath))
    }
  }

  it should "process static data in configurable batch sizes" in {
    val testDataPath = createTestDataset(1000)
    val batchSize = 250
    
    try {
      val result = repository.processInBatches(testDataPath, batchSize)
      
      result shouldBe a[Success[_]]
      result.get.size should be >= 900 // Most records should be processed
      
    } finally {
      Files.deleteIfExists(Paths.get(testDataPath))
    }
  }

  private def createTestDataset(recordCount: Int): String = {
    val tempFile = Files.createTempFile("test-lastfm-", ".tsv")
    val testData = (1 to recordCount).map { i =>
      val userId = s"user_$i"
      val timestamp = Instant.now().plusSeconds(i).toString
      val artistId = if (i % 3 == 0) s"artist_$i" else ""
      val artistName = s"Artist $i"
      val trackId = if (i % 2 == 0) s"track_$i" else ""
      val trackName = s"Track $i"
      
      s"$userId\t$timestamp\t$artistId\t$artistName\t$trackId\t$trackName"
    }.mkString("\n")
    
    Files.write(tempFile, testData.getBytes(StandardCharsets.UTF_8))
    tempFile.toString
  }
}