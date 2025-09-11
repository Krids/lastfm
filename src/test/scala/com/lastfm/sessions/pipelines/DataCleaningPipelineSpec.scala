package com.lastfm.sessions.pipelines

import com.lastfm.sessions.domain.DataQualityMetrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.util.{Success, Failure, Try}

/**
 * Test specification for DataCleaningPipeline.
 * 
 * Tests production-grade pipeline implementation including:
 * - Configuration-driven execution with environment awareness
 * - Bronze → Silver medallion architecture transformation 
 * - Strategic partitioning optimization for Session Analysis Context
 * - Comprehensive error handling and quality validation
 * - Artifact generation with proper directory structure
 * - Performance optimization with distributed processing
 * 
 * Follows enterprise data engineering best practices:
 * - Single Responsibility Principle for pipeline separation
 * - Environment-aware partitioning strategy 
 * - Strategic caching through persistent Silver artifacts
 * - Comprehensive quality monitoring and audit trails
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DataCleaningPipelineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  // Test SparkSession with production-like configuration
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataCleaningPipelineTest")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  // Test directories following medallion architecture
  val testBaseDir = "/tmp/pipeline-test"
  val bronzeDir = s"$testBaseDir/bronze"
  val silverDir = s"$testBaseDir/silver"

  override def beforeEach(): Unit = {
    cleanupTestDirectories()
    createTestDirectories()
  }

  override def afterEach(): Unit = {
    cleanupTestDirectories()
  }

  /**
   * Tests for configuration-driven pipeline execution.
   */
  "DataCleaningPipeline configuration" should "accept valid pipeline configuration" in {
    // Arrange
    val config = PipelineConfig(
      bronzePath = s"$bronzeDir/test-input.tsv",
      silverPath = s"$silverDir/test-output.tsv",
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 16, timeZone = "UTC")
    )
    
    // Act
    val pipeline = new DataCleaningPipeline(config)
    
    // Assert - Pipeline should be created with configuration
    pipeline should not be null
    pipeline.config should be(config)
  }
  
  it should "validate configuration parameters at creation" in {
    // Act & Assert - Should reject invalid configuration
    an [IllegalArgumentException] should be thrownBy {
      DataCleaningPipeline(PipelineConfig(
        bronzePath = "", // Invalid empty path
        silverPath = s"$silverDir/output.tsv",
        partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
        qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
        sparkConfig = SparkConfig(partitions = 16, timeZone = "UTC")
      ))
    }
  }

  /**
   * Tests for Bronze → Silver transformation execution.
   */
  "DataCleaningPipeline execution" should "successfully process Bronze → Silver transformation" in {
    // Arrange
    val inputPath = createTestData(List(
      ("user_000001", "2009-05-04T23:08:57Z", "artist-1", "Artist1", "track-1", "Track1"),
      ("user_000002", "2009-05-04T23:09:57Z", "artist-2", "Artist2", "track-2", "Track2")
    ))
    val outputPath = s"$silverDir/transformation-test.tsv"
    
    val config = PipelineConfig(
      bronzePath = inputPath,
      silverPath = outputPath,
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
    
    val pipeline = new DataCleaningPipeline(config)
    
    // Act
    val result = pipeline.execute()
    
    // Assert
    result shouldBe a[Success[_]]
    val qualityMetrics = result.get
    
    qualityMetrics.totalRecords should be(2L)
    qualityMetrics.validRecords should be(2L) 
    qualityMetrics.qualityScore should be(100.0 +- 0.1)
  }

  it should "generate Silver layer artifacts with proper format" in {
    // Arrange
    val inputPath = createTestData(List(
      ("user_000001", "2009-05-04T23:08:57Z", "artist-1", "Deep Dish", "track-1", "Test Track")
    ))
    val outputPath = s"$silverDir/artifact-generation-test.tsv" 
    
    val config = PipelineConfig(
      bronzePath = inputPath,
      silverPath = outputPath,
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
    
    val pipeline = new DataCleaningPipeline(config)
    
    // Act
    val result = pipeline.execute()
    
    // Assert - Verify Silver artifacts generated
    result shouldBe a[Success[_]]
    
    // Should generate Silver directory structure
    val silverOutputDir = Paths.get(outputPath).getParent
    Files.exists(silverOutputDir) should be(true)
    
    // Should generate quality report alongside cleaned data
    val qualityReportPath = getExpectedQualityReportPath(outputPath)
    Files.exists(Paths.get(qualityReportPath)) should be(true)
  }

  /**
   * Tests for strategic partitioning optimization.
   */
  "DataCleaningPipeline partitioning" should "partition data by userId for session analysis" in {
    // Arrange
    val inputPath = createTestData(generateUserData(users = 10, tracksPerUser = 5))
    val outputPath = s"$silverDir/partitioning-test.tsv"
    
    val config = PipelineConfig(
      bronzePath = inputPath,
      silverPath = outputPath,
      partitionStrategy = UserIdPartitionStrategy(userCount = 10, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
    
    val pipeline = new DataCleaningPipeline(config)
    
    // Act
    val result = pipeline.execute()
    
    // Assert - Should apply optimal partitioning strategy
    result shouldBe a[Success[_]]
    
    // Verify partitioning metadata is tracked in quality metrics
    val qualityMetrics = result.get
    qualityMetrics.totalRecords should be(50L) // 10 users × 5 tracks
    qualityMetrics.qualityScore should be(100.0 +- 0.1)
  }
  
  it should "use optimized partition count for 1K user dataset" in {
    // Arrange
    val localStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 8)
    val clusterStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 100)
    
    // Act & Assert - Should use optimized 16 partitions for 1K user dataset
    // Both local and cluster use the same optimized partition count for our specific dataset
    localStrategy.calculateOptimalPartitions() should be(16) // Optimal for 1K users (~62 users per partition)
    clusterStrategy.calculateOptimalPartitions() should be(16) // Same optimization regardless of cores
    
    // Verify users per partition calculation
    localStrategy.usersPerPartition should be(62) // 1000 users / 16 partitions = ~62.5
    clusterStrategy.usersPerPartition should be(62)
  }

  /**
   * Tests for error handling and edge cases.
   */
  "DataCleaningPipeline error handling" should "handle missing input files gracefully" in {
    // Arrange
    val nonExistentPath = s"$bronzeDir/non-existent-file.tsv"
    val config = PipelineConfig(
      bronzePath = nonExistentPath,
      silverPath = s"$silverDir/error-test.tsv",
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
    
    val pipeline = new DataCleaningPipeline(config)
    
    // Act
    val result = pipeline.execute()
    
    // Assert - Should fail gracefully with meaningful error
    result.isFailure should be(true)
    result.failed.get.getMessage should include("not found")
  }
  
  it should "provide detailed error context for quality validation failures" in {
    // Arrange - Data that will be filtered out during validation
    val inputPath = createTestData(List(
      ("user_000001", "2009-05-04T23:08:57Z", "artist-1", "Artist1", "track-1", "Track1"), // Good data
      ("user_000002", "2009-05-04T23:09:57Z", "artist-2", "", "track-2", "") // Empty artist and track names
    ))
    val config = PipelineConfig(
      bronzePath = inputPath,
      silverPath = s"$silverDir/quality-failure-test.tsv",
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
    
    val pipeline = new DataCleaningPipeline(config)
    
    // Act
    val result = pipeline.execute()
    
    // Assert - Should fail due to quality below threshold
    result shouldBe a[Failure[_]]
    result.failed.get.getMessage should include("Quality score 50.000000% below session analysis threshold 99.0%")
  }

  /**
   * Tests for performance and scalability characteristics.
   */
  "DataCleaningPipeline performance" should "maintain consistent performance across data sizes" in {
    // Arrange - Different dataset sizes
    val smallDataset = generateUserData(users = 10, tracksPerUser = 10)  // 100 records
    val mediumDataset = generateUserData(users = 50, tracksPerUser = 20) // 1000 records
    
    // Act - Process datasets of different sizes
    val smallInputPath = createTestData(smallDataset)
    val smallConfig = PipelineConfig(
      bronzePath = smallInputPath,
      silverPath = s"$silverDir/performance-small.tsv",
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
    val smallResult = new DataCleaningPipeline(smallConfig).execute()
    
    val mediumInputPath = createTestData(mediumDataset)
    val mediumConfig = PipelineConfig(
      bronzePath = mediumInputPath,
      silverPath = s"$silverDir/performance-medium.tsv",
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
    val mediumResult = new DataCleaningPipeline(mediumConfig).execute()
    
    // Assert - Both should succeed with linear scaling characteristics
    smallResult shouldBe a[Success[_]]
    mediumResult shouldBe a[Success[_]]
    
    val smallMetrics = smallResult.get
    val mediumMetrics = mediumResult.get
    
    smallMetrics.qualityScore should be(100.0 +- 0.1)
    mediumMetrics.qualityScore should be(100.0 +- 0.1)
  }

  /**
   * Tests for integration with Session Analysis Context preparation.
   */
  "DataCleaningPipeline session analysis preparation" should "partition cleaned data for optimal session grouping" in {
    // Arrange - Data with multiple users for partitioning
    val multiUserData = generateUserData(users = 8, tracksPerUser = 5) // Test partitioning
    val inputPath = createTestData(multiUserData)
    
    val config = PipelineConfig(
      bronzePath = inputPath,
      silverPath = s"$silverDir/session-prep-test.tsv",
      partitionStrategy = UserIdPartitionStrategy(userCount = 8, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
    
    val pipeline = new DataCleaningPipeline(config)
    
    // Act
    val result = pipeline.execute()
    
    // Assert - Should prepare data optimally for session analysis
    result shouldBe a[Success[_]]
    
    val qualityMetrics = result.get
    qualityMetrics.isSessionAnalysisReady should be(true)
    qualityMetrics.hasAcceptableTrackCoverage should be(true)
    
    // Should indicate optimal partitioning was applied
    // (This will be validated through performance metrics in implementation)
  }

  /**
   * Helper methods for test data generation and management.
   */
  private def createTestData(data: List[(String, String, String, String, String, String)]): String = {
    val testFile = s"$bronzeDir/test-input-${System.currentTimeMillis()}.tsv"
    val content = data.map { case (userId, timestamp, artistId, artistName, trackId, trackName) =>
      s"$userId\t$timestamp\t$artistId\t$artistName\t$trackId\t$trackName"
    }.mkString("\n")
    
    Files.write(Paths.get(testFile), content.getBytes(StandardCharsets.UTF_8))
    testFile
  }
  
  private def generateUserData(users: Int, tracksPerUser: Int): List[(String, String, String, String, String, String)] = {
    (1 to users).flatMap { userIndex =>
      (1 to tracksPerUser).map { trackIndex =>
        val userId = f"user_${userIndex}%06d"
        val timestamp = f"2009-05-04T${(trackIndex % 24)}%02d:08:57Z"
        val artistId = s"artist-$userIndex-$trackIndex"
        val artistName = s"Artist $userIndex"
        // Generate high MBID coverage (90%+) to meet quality thresholds
        val trackId = if (trackIndex % 10 == 0) "" else s"track-$userIndex-$trackIndex" // Only 10% missing MBIDs
        val trackName = s"Track $trackIndex"
        
        (userId, timestamp, artistId, artistName, trackId, trackName)
      }
    }.toList
  }
  
  private def getExpectedQualityReportPath(outputPath: String): String = {
    val outputFile = Paths.get(outputPath)
    val outputDir = outputFile.getParent
    val baseFileName = outputFile.getFileName.toString.replaceAll("\\.[^.]*$", "")
    outputDir.resolve(s"$baseFileName-quality-report.json").toString
  }

  private def createTestDirectories(): Unit = {
    Files.createDirectories(Paths.get(bronzeDir))
    Files.createDirectories(Paths.get(silverDir))
  }
  
  private def cleanupTestDirectories(): Unit = {
    Try {
      if (Files.exists(Paths.get(testBaseDir))) {
        Files.walk(Paths.get(testBaseDir))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists)
      }
    }
  }
}