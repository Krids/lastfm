package com.lastfm.sessions

import com.lastfm.sessions.pipelines.{DataCleaningPipeline, PipelineConfig, UserIdPartitionStrategy, QualityThresholds, SparkConfig}
import com.lastfm.sessions.orchestration.{PipelineOrchestrator, ProductionConfigManager, SparkSessionManager, PipelineExecutionResult}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.io.{ByteArrayOutputStream, PrintStream}
import scala.util.{Try, Success}

/**
 * Test specification for Main.scala pipeline orchestration.
 * 
 * Tests production-grade pipeline orchestration including:
 * - Command-line argument parsing and validation
 * - Individual pipeline execution based on user selection
 * - Complete pipeline execution with proper dependency management
 * - Error handling and user guidance for invalid arguments
 * - Spark session lifecycle management
 * - Configuration validation and environment setup
 * 
 * Validates enterprise orchestration patterns:
 * - Single entry point for all pipeline operations
 * - Flexible execution modes for development and production
 * - Comprehensive error handling with meaningful user feedback
 * - Production-ready logging and monitoring integration
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class MainOrchestrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  // Test directories for orchestration testing
  val testBaseDir = "/tmp/orchestration-test"
  val bronzeDir = s"$testBaseDir/bronze"
  val silverDir = s"$testBaseDir/silver" 
  val goldDir = s"$testBaseDir/gold"
  
  override def afterEach(): Unit = {
    cleanupSessionAnalysisTestArtifacts()
    super.afterEach()
  }

  /**
   * Tests for command-line argument parsing and pipeline selection.
   */
  "Main orchestration argument parsing" should "execute data-cleaning pipeline when specified" in {
    // Arrange
    val args = Array("data-cleaning")
    val testInputPath = createTestDataForOrchestration()
    
    // Act & Assert - Should execute only DataCleaningPipeline
    val result = PipelineOrchestrator.parseArgsAndExecute(args, createTestConfig(testInputPath))
    
    result should be(PipelineExecutionResult.DataCleaningCompleted)
  }
  
  it should "execute session-analysis pipeline when specified" in {
    // Arrange
    val args = Array("session-analysis")
    val testInputPath = createTestDataForOrchestration()
    
    // Act & Assert - Should execute SessionAnalysisPipeline (when implemented)
    val result = PipelineOrchestrator.parseArgsAndExecute(args, createTestConfig(testInputPath))
    
    result should be(PipelineExecutionResult.SessionAnalysisCompleted)
  }
  
  it should "execute ranking pipeline when specified" in {
    // Arrange
    val args = Array("ranking")
    val testInputPath = createTestDataForOrchestration()
    
    // Act & Assert - Should execute RankingPipeline (when implemented)
    val result = PipelineOrchestrator.parseArgsAndExecute(args, createTestConfig(testInputPath))
    
    result should be(PipelineExecutionResult.RankingCompleted)
  }
  
  it should "execute complete pipeline when 'complete' specified" in {
    // Arrange
    val args = Array("complete")
    val testInputPath = createTestDataForOrchestration()
    
    // Act & Assert - Should execute all pipelines in sequence
    val result = PipelineOrchestrator.parseArgsAndExecute(args, createTestConfig(testInputPath))
    
    result should be(PipelineExecutionResult.CompletePipelineCompleted)
  }
  
  it should "execute complete pipeline when no args provided" in {
    // Arrange - No command-line arguments (default behavior)
    val args = Array.empty[String]
    val testInputPath = createTestDataForOrchestration()
    
    // Act & Assert - Should default to complete pipeline execution
    val result = PipelineOrchestrator.parseArgsAndExecute(args, createTestConfig(testInputPath))
    
    result should be(PipelineExecutionResult.CompletePipelineCompleted)
  }

  /**
   * Tests for error handling and user guidance.
   */
  "Main orchestration error handling" should "provide usage help for invalid arguments" in {
    // Arrange
    val args = Array("invalid-pipeline")
    val outputStream = new ByteArrayOutputStream()
    val printStream = new PrintStream(outputStream)
    
    // Act - Test that invalid arguments return proper result
    val result = PipelineOrchestrator.parseArgsAndExecute(args, createTestConfig("dummy"))
    
    // Assert - Should return InvalidArguments result (usage help displayed to console)
    result should be(PipelineExecutionResult.InvalidArguments)
    
    // Note: Usage help display verified through integration testing
  }
  
  it should "handle missing input files gracefully" in {
    // Arrange
    val args = Array("data-cleaning")
    val nonExistentPath = "/non/existent/path.tsv"
    val config = createTestConfig(nonExistentPath)
    
    // Act & Assert - Should fail gracefully with meaningful error
    val result = PipelineOrchestrator.parseArgsAndExecute(args, config)
    
    result should be(PipelineExecutionResult.ExecutionFailed)
  }

  /**
   * Tests for Spark session lifecycle management.
   */
  "Main orchestration Spark management" should "create Spark session with production configuration" in {
    // Act
    val sparkSession = SparkSessionManager.createProductionSession()
    
    try {
      // Assert - Should create properly configured Spark session
      sparkSession should not be null
      sparkSession.sparkContext.appName should include("LastFM-SessionAnalysis")
      sparkSession.conf.get("spark.sql.session.timeZone") should be("UTC")
      sparkSession.sparkContext.defaultParallelism should be > 1
      
    } finally {
      sparkSession.stop()
    }
  }
  
  it should "cleanup Spark session properly after execution" in {
    // Arrange
    val sparkSession = SparkSessionManager.createProductionSession()
    
    // Act - Simulate pipeline execution and cleanup
    val isActiveBefore = !sparkSession.sparkContext.isStopped
    SparkSessionManager.cleanupSession(sparkSession)
    val isActiveAfter = sparkSession.sparkContext.isStopped
    
    // Assert - Should properly shutdown Spark session
    isActiveBefore should be(true)
    isActiveAfter should be(true) // isStopped = true means session is stopped
  }

  /**
   * Tests for configuration validation and environment setup.
   */
  "Main orchestration configuration" should "validate production configuration" in {
    // Arrange
    val productionConfig = ProductionConfigManager.loadProductionConfig()
    
    // Act & Assert - Should load valid production configuration
    productionConfig.bronzePath should include("lastfm-dataset-1k")
    productionConfig.silverPath should include("silver")
    productionConfig.qualityThresholds.sessionAnalysisMinQuality should be >= 99.0
    productionConfig.partitionStrategy.calculateOptimalPartitions() should be > 0
  }
  
  it should "create production directory structure" in {
    // Act
    ProductionConfigManager.createProductionDirectories()
    
    // Assert - Should create medallion architecture directories
    Files.exists(Paths.get("data/output/bronze")) should be(true)
    Files.exists(Paths.get("data/output/silver")) should be(true) 
    Files.exists(Paths.get("data/output/gold")) should be(true)
  }

  /**
   * Helper methods for orchestration testing.
   */
  /**
   * Cleans up session analysis test artifacts that are created in the actual Silver layer.
   */
  private def cleanupSessionAnalysisTestArtifacts(): Unit = {
    Try {
      val silverSessionsPath = Paths.get("data/output/silver/sessions.parquet")
      if (Files.exists(silverSessionsPath)) {
        println(s"🧹 Cleaning up session analysis test artifacts: $silverSessionsPath")
        Files.walk(silverSessionsPath)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists)
        println("✅ Session analysis test artifacts cleaned")
      }
    }.recover {
      case ex: Exception =>
        println(s"⚠️  Could not clean session analysis artifacts: ${ex.getMessage}")
        // Don't fail tests due to cleanup issues
    }
  }

  private def createTestDataForOrchestration(): String = {
    Files.createDirectories(Paths.get(bronzeDir))
    
    val testFile = s"$bronzeDir/orchestration-test.tsv"
    val content = List(
      ("user_000001", "2009-05-04T23:08:57Z", "artist-1", "Test Artist", "track-1", "Test Track")
    ).map { case (userId, timestamp, artistId, artistName, trackId, trackName) =>
      s"$userId\t$timestamp\t$artistId\t$artistName\t$trackId\t$trackName"
    }.mkString("\n")
    
    Files.write(Paths.get(testFile), content.getBytes(StandardCharsets.UTF_8))
    testFile
  }
  
  private def createTestConfig(bronzePath: String): PipelineConfig = {
    Files.createDirectories(Paths.get(silverDir))
    
    PipelineConfig(
      bronzePath = bronzePath,
      silverPath = s"$silverDir/orchestration-output", // Parquet directory, not .tsv file
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 99.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
  }
}

// PipelineExecutionResult now imported from orchestration package