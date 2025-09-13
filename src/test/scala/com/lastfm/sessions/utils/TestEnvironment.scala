package com.lastfm.sessions.utils

import java.nio.file.{Files, Path, Paths}
import scala.util.Try
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
 * Test environment setup and cleanup trait for LastFM session tests.
 * 
 * Provides isolated test directories to prevent interference with production data.
 * All test artifacts are written to data/test/ and cleaned up after tests.
 * 
 * ⚠️  IMPORTANT: This trait is now integrated with BaseTestSpec for enhanced safety.
 * For new tests, extend BaseTestSpec instead of using TestEnvironment directly.
 * 
 * Directory Structure:
 * - data/test/bronze/   - Test input files
 * - data/test/silver/   - Test intermediate data (cleaned events, sessions)
 * - data/test/gold/     - Test analysis outputs
 * - data/test/results/  - Test final outputs (TSV files)
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait TestEnvironment extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>
  
  // Test directory paths - isolated from production
  val testRootDir = "data/test"
  val testBronzeDir = s"$testRootDir/bronze"
  val testSilverDir = s"$testRootDir/silver"
  val testGoldDir = s"$testRootDir/gold"
  val testResultsDir = s"$testRootDir/results"
  
  // Legacy test directories that some tests might still use
  val legacyTestDir = "/tmp/orchestration-test"
  
  /**
   * Create test directory structure before all tests.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestDirectories()
  }
  
  /**
   * Clean up test artifacts after each test to ensure isolation.
   */
  override def afterEach(): Unit = {
    cleanTestArtifacts()
    super.afterEach()
  }
  
  /**
   * Final cleanup after all tests complete.
   */
  override def afterAll(): Unit = {
    cleanupAllTestDirectories()
    super.afterAll()
  }
  
  /**
   * Creates the test directory structure.
   */
  protected def createTestDirectories(): Unit = {
    val directories = List(
      testBronzeDir,
      testSilverDir, 
      testGoldDir,
      testResultsDir
    )
    
    directories.foreach { dir =>
      Try {
        Files.createDirectories(Paths.get(dir))
      }.recover {
        case ex: Exception =>
          println(s"⚠️ Could not create test directory $dir: ${ex.getMessage}")
      }
    }
  }
  
  /**
   * Cleans test artifacts from the current test.
   * Preserves directory structure for next test.
   */
  protected def cleanTestArtifacts(): Unit = {
    // Clean contents but preserve directories
    val dirsToClean = List(
      testSilverDir,
      testGoldDir,
      testResultsDir
    )
    
    dirsToClean.foreach { dir =>
      cleanDirectoryContents(Paths.get(dir))
    }
    
    // Also clean any production artifacts that tests might have created
    cleanProductionTestArtifacts()
  }
  
  /**
   * Final cleanup - removes entire test directory tree.
   */
  protected def cleanupAllTestDirectories(): Unit = {
    Try {
      val testRoot = Paths.get(testRootDir)
      if (Files.exists(testRoot)) {
        Files.walk(testRoot)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists)
      }
    }.recover {
      case ex: Exception =>
        println(s"⚠️ Could not clean test root directory: ${ex.getMessage}")
    }
    
    // Clean legacy test directory
    Try {
      val legacyPath = Paths.get(legacyTestDir)
      if (Files.exists(legacyPath)) {
        Files.walk(legacyPath)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists)
      }
    }.recover {
      case ex: Exception =>
        println(s"⚠️ Could not clean legacy test directory: ${ex.getMessage}")
    }
  }
  
  /**
   * Cleans directory contents but preserves the directory itself.
   */
  private def cleanDirectoryContents(dir: Path): Unit = {
    Try {
      if (Files.exists(dir) && Files.isDirectory(dir)) {
        Files.list(dir).forEach { path =>
          Try {
            if (Files.isDirectory(path)) {
              Files.walk(path)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(Files.deleteIfExists)
            } else {
              Files.deleteIfExists(path)
            }
          }.recover {
            case ex: Exception =>
              println(s"⚠️ Could not clean $path: ${ex.getMessage}")
          }
        }
      }
    }.recover {
      case ex: Exception =>
        println(s"⚠️ Could not clean directory $dir: ${ex.getMessage}")
    }
  }
  
  /**
   * Cleans any production artifacts that tests might have accidentally created.
   * This ensures production directories remain clean.
   */
  private def cleanProductionTestArtifacts(): Unit = {
    // Clean sessions.parquet from production Silver layer if it exists
    val productionSessionsPath = Paths.get("data/output/silver/sessions.parquet")
    if (Files.exists(productionSessionsPath)) {
      Try {
        println(s"🧹 Cleaning production test artifact: $productionSessionsPath")
        Files.walk(productionSessionsPath)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists)
      }.recover {
        case ex: Exception =>
          println(s"⚠️ Could not clean production sessions: ${ex.getMessage}")
      }
    }
    
    // Clean any test-generated reports from production folders
    val testReportPatterns = List(
      "data/output/silver/test-",
      "data/output/gold/test-",
      "data/output/results/test-"
    )
    
    testReportPatterns.foreach { pattern =>
      val dir = Paths.get(pattern.substring(0, pattern.lastIndexOf("/")))
      if (Files.exists(dir) && Files.isDirectory(dir)) {
        Try {
          Files.list(dir)
            .filter(p => p.getFileName.toString.startsWith("test-"))
            .forEach { path =>
              Try(Files.deleteIfExists(path))
            }
        }
      }
    }
  }
  
  /**
   * Creates a test configuration that uses test directories.
   * 
   * ⚠️  DEPRECATED: Use TestConfiguration.testConfig() instead for new tests.
   * This method is maintained for backward compatibility with existing tests.
   */
  protected def createTestPipelineConfig(
    bronzePath: String = s"$testBronzeDir/test-input.tsv",
    silverPath: String = s"$testSilverDir/test-output.parquet"
  ): com.lastfm.sessions.pipelines.PipelineConfig = {
    import com.lastfm.sessions.pipelines._
    
    // Validate paths are test-isolated
    require(bronzePath.startsWith("data/test"), 
      s"CRITICAL: Test bronze path must use data/test/, got: $bronzePath")
    require(silverPath.startsWith("data/test"), 
      s"CRITICAL: Test silver path must use data/test/, got: $silverPath")
    
    PipelineConfig(
      bronzePath = bronzePath,
      silverPath = silverPath,
      goldPath = testGoldDir,
      outputPath = testResultsDir,
      partitionStrategy = UserIdPartitionStrategy(userCount = 100, cores = 4),
      qualityThresholds = QualityThresholds(sessionAnalysisMinQuality = 95.0),
      sparkConfig = SparkConfig(partitions = 8, timeZone = "UTC")
    )
  }
  
  /**
   * Gets test-isolated configuration using the new safety framework.
   * This method bridges legacy TestEnvironment with new TestConfiguration.
   */
  protected def getTestConfig(): com.lastfm.sessions.config.AppConfiguration = {
    TestConfiguration.testConfig()
  }
  
  /**
   * Creates sample test data in the test bronze directory.
   */
  protected def createTestData(fileName: String = "test-input.tsv"): String = {
    val testFile = s"$testBronzeDir/$fileName"
    val content = List(
      "user_000001\t2009-05-01T09:00:00Z\ta1b1cf71-bd35-4e99-8624-24a6e15f133a\tDeep Dish\tf1b1cf71-bd35-4e99-8624-24a6e15f133a\tFlashdance",
      "user_000001\t2009-05-01T09:10:00Z\ta1b1cf72-bd35-4e99-8624-24a6e15f133b\tKing Crimson\tf1b1cf72-bd35-4e99-8624-24a6e15f133b\tEpitaph",
      "user_000001\t2009-05-01T09:35:00Z\ta1b1cf73-bd35-4e99-8624-24a6e15f133c\tThe Knife\tf1b1cf73-bd35-4e99-8624-24a6e15f133c\tHeartbeats",
      "user_000002\t2009-05-01T10:00:00Z\ta1b1cf74-bd35-4e99-8624-24a6e15f133d\tCake\tf1b1cf74-bd35-4e99-8624-24a6e15f133d\tJolene"
    ).mkString("\n")
    
    Files.createDirectories(Paths.get(testBronzeDir))
    Files.write(Paths.get(testFile), content.getBytes)
    testFile
  }
}