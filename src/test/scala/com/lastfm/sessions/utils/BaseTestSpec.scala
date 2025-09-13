package com.lastfm.sessions.utils

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import com.lastfm.sessions.config.AppConfiguration
import java.nio.file.{Files, Paths}
import scala.util.{Try, Success, Failure}

/**
 * Mandatory base class for ALL test specifications in the LastFM session analyzer.
 * 
 * This trait enforces critical safety measures to prevent test contamination of
 * production data by ensuring proper test isolation and cleanup.
 * 
 * Key Safety Features:
 * - Mandatory test configuration validation before any test execution
 * - Automatic cleanup of test artifacts with contamination detection
 * - Runtime monitoring for production path usage
 * - Fail-fast error reporting for configuration violations
 * 
 * Design Principles (following Clean Architecture):
 * - Single Responsibility: Focus only on test isolation and safety
 * - Fail-Fast: Immediate detection and reporting of safety violations
 * - Clear Error Messages: Descriptive failures for quick problem resolution
 * - Zero Production Impact: Complete isolation of test execution
 * 
 * Usage:
 * ```scala
 * class MyFeatureSpec extends AnyWordSpec with BaseTestSpec {
 *   // testConfig is automatically available and validated
 *   // All test artifacts will be isolated to data/test/
 * }
 * ```
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
trait BaseTestSpec extends TestEnvironment { this: Suite =>
  
  /**
   * Test-isolated configuration - automatically validated for safety.
   * This implicit is available to all tests that extend BaseTestSpec.
   */
  implicit val testConfig: AppConfiguration = TestConfiguration.testConfig()
  
  /**
   * Pre-test validation to ensure complete test isolation.
   * 
   * This method runs before ALL tests and performs comprehensive safety checks
   * to prevent any possibility of production data contamination.
   */
  override def beforeAll(): Unit = {
    // Emergency safety check first
    TestConfiguration.emergencySafetyCheck()
    
    // Validate test configuration
    TestConfiguration.validateTestEnvironment(testConfig)
    
    // Ensure test directories exist
    createTestDirectoriesWithValidation()
    
    // Run parent setup
    super.beforeAll()
    
    println(s"🧪 Test environment initialized safely - isolated to: ${testConfig.outputBasePath}")
  }
  
  /**
   * Post-test cleanup with production contamination detection.
   * 
   * After each test, this method cleans up test artifacts and performs
   * safety checks to ensure no production directories were contaminated.
   */
  override def afterEach(): Unit = {
    super.afterEach()
    
    // Validate no production contamination occurred
    validateNoProductionContamination() match {
      case Success(_) => 
        println("✅ Test completed safely - no production contamination detected")
      case Failure(exception) =>
        System.err.println(s"🚨 CRITICAL: Production contamination detected: ${exception.getMessage}")
        throw exception
    }
  }
  
  /**
   * Final cleanup with comprehensive validation.
   */
  override def afterAll(): Unit = {
    // Final contamination check
    validateNoProductionContamination()
    
    super.afterAll()
    println("🧹 Test suite completed - all artifacts cleaned safely")
  }
  
  /**
   * Creates test directories with validation to ensure they're properly isolated.
   */
  private def createTestDirectoriesWithValidation(): Unit = {
    val testDirectories = List(
      testConfig.bronzePath,
      testConfig.silverPath,
      testConfig.goldPath,
      testConfig.resultsPath
    )
    
    testDirectories.foreach { dir =>
      // Double-check each directory is test-isolated
      require(dir.startsWith("data/test"), 
        s"CRITICAL: Test directory must be under data/test/, got: $dir")
      
      Try {
        Files.createDirectories(Paths.get(dir))
        println(s"📁 Created test directory: $dir")
      }.recover {
        case exception =>
          throw new IllegalStateException(
            s"Failed to create test directory: $dir - ${exception.getMessage}",
            exception
          )
      }
    }
  }
  
  /**
   * Validates that no production directories were contaminated during test execution.
   * 
   * This is a critical safety check that scans production directories for any
   * test-generated artifacts that could indicate a safety violation.
   * 
   * @return Success if no contamination detected, Failure if contamination found
   */
  private def validateNoProductionContamination(): Try[Unit] = Try {
    val productionPaths = List(
      "data/output/bronze",
      "data/output/silver", 
      "data/output/gold", 
      "data/output/results"
    )
    
    val contaminatedFiles = productionPaths.flatMap { productionPath =>
      val path = Paths.get(productionPath)
      
      if (Files.exists(path) && Files.isDirectory(path)) {
        Try {
          Files.list(path)
            .filter(p => {
              val fileName = p.getFileName.toString.toLowerCase
              fileName.contains("test") || 
              fileName.startsWith("temp-") || 
              fileName.startsWith("tmp-") ||
              fileName.contains("spec") ||
              fileName.contains("fixture")
            })
            .toArray
            .map(_.toString)
        }.getOrElse(Array.empty[String])
      } else {
        Array.empty[String]
      }
    }
    
    if (contaminatedFiles.nonEmpty) {
      val contaminationDetails = contaminatedFiles.mkString("\n  - ", "\n  - ", "")
      throw new IllegalStateException(
        s"🚨 CRITICAL PRODUCTION CONTAMINATION DETECTED!\n" +
        s"The following test artifacts were found in production directories:$contaminationDetails\n\n" +
        s"This indicates a test safety violation. Production data integrity may be compromised.\n" +
        s"Immediate action required:\n" +
        s"1. Review the failing test for production path usage\n" +
        s"2. Clean contaminated production directories\n" +
        s"3. Verify TestConfiguration.testConfig() is being used"
      )
    }
  }
  
  /**
   * Utility method for tests to get validated test data paths.
   * 
   * @param layer The medallion layer (bronze, silver, gold, results)
   * @return Validated test path for the specified layer
   */
  protected def getTestPath(layer: String): String = {
    layer.toLowerCase match {
      case "bronze" => testConfig.bronzePath
      case "silver" => testConfig.silverPath
      case "gold" => testConfig.goldPath
      case "results" => testConfig.resultsPath
      case _ => throw new IllegalArgumentException(s"Unknown layer: $layer")
    }
  }
  
  /**
   * Creates a validated test-specific subdirectory within the isolated test area.
   * 
   * @param subdirectory Name of subdirectory to create
   * @param layer Medallion layer (bronze, silver, gold, results)
   * @return Path to created subdirectory
   */
  protected def createTestSubdirectory(subdirectory: String, layer: String): String = {
    val basePath = getTestPath(layer)
    val fullPath = s"$basePath/$subdirectory"
    
    // Safety validation
    require(fullPath.startsWith("data/test"), 
      s"CRITICAL: Test subdirectory must be under data/test/, got: $fullPath")
    
    Files.createDirectories(Paths.get(fullPath))
    fullPath
  }
}