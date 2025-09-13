package com.lastfm.sessions.utils

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Critical safety specification that validates test isolation infrastructure.
 * 
 * This test suite ensures that the test isolation framework is working correctly
 * and that production data contamination risks are properly mitigated.
 * 
 * IMPORTANT: This test itself demonstrates proper usage of the isolation framework.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TestIsolationSpec extends AnyWordSpec with BaseTestSpec with Matchers {

  "Test Isolation Framework" should {
    
    "provide test-isolated configuration" in {
      // Verify implicit config is test-isolated
      testConfig.isTest should be(true)
      testConfig.environment should be("test")
      testConfig.outputBasePath should startWith("data/test")
      
      // Verify all output paths are isolated
      testConfig.bronzePath should startWith("data/test")
      testConfig.silverPath should startWith("data/test")
      testConfig.goldPath should startWith("data/test")  
      testConfig.resultsPath should startWith("data/test")
    }
    
    "prevent production path usage in test configuration" in {
      // These paths should NEVER appear in test configuration
      testConfig.outputBasePath should not contain("data/output")
      testConfig.silverPath should not contain("data/output")
      testConfig.goldPath should not contain("data/output")
      testConfig.resultsPath should not contain("data/output")
    }
    
    "create test directories on demand" in {
      val testSubdir = createTestSubdirectory("isolation-test", "silver")
      
      testSubdir should startWith("data/test")
      Files.exists(Paths.get(testSubdir)) should be(true)
      
      // Verify it's actually a test directory
      testSubdir should include("data/test/silver/isolation-test")
    }
    
    "validate TestConfiguration safety checks" in {
      // This should work without throwing
      val config = TestConfiguration.testConfig()
      config.isTest should be(true)
      
      // This should validate successfully
      noException should be thrownBy TestConfiguration.validateTestEnvironment(config)
    }
    
    "reject unsafe configuration attempts" in {
      // Attempting to create production-path config should fail
      an[IllegalStateException] should be thrownBy {
        TestConfiguration.testConfigWithOverrides(Map(
          "data.output.base" -> "data/output"  // This should fail in test environment
        ))
      }
    }
    
    "validate path safety with PipelineConfig integration" in {
      val pipelineConfig = testConfig.toPipelineConfig
      
      // Verify all paths are test-isolated
      pipelineConfig.silverPath should startWith("data/test")
      pipelineConfig.goldPath should startWith("data/test")
      pipelineConfig.outputPath should startWith("data/test")
      
      // This should pass without throwing
      noException should be thrownBy {
        pipelineConfig.validateSafeExecution("test validation")
      }
    }
  }
  
  "Emergency Safety Mechanisms" should {
    
    "detect production configuration misuse" in {
      // Emergency safety check should pass in test environment
      noException should be thrownBy TestConfiguration.emergencySafetyCheck()
    }
    
    "provide clear error messages for configuration violations" in {
      val exception = intercept[IllegalStateException] {
        TestConfiguration.testConfigWithOverrides(Map(
          "environment" -> "production"  // This should fail
        ))
      }
      
      exception.getMessage should include("CRITICAL SAFETY VIOLATION")
      exception.getMessage should include("test")
    }
  }
  
  "Framework Integration" should {
    
    "work with existing TestEnvironment" in {
      // Test that our new framework integrates with existing code
      val legacyConfig = createTestPipelineConfig()
      
      legacyConfig.silverPath should startWith("data/test")
      legacyConfig.goldPath should startWith("data/test")
      legacyConfig.outputPath should startWith("data/test")
    }
    
    "provide backward compatibility" in {
      // Legacy directory constants should still work but be isolated
      testBronzeDir should startWith("data/test")
      testSilverDir should startWith("data/test")
      testGoldDir should startWith("data/test")
      testResultsDir should startWith("data/test")
    }
  }
  
  "Data Isolation Verification" should {
    
    "ensure test directories are separate from production" in {
      // Get paths for all layers
      val testPaths = List(
        getTestPath("bronze"),
        getTestPath("silver"),
        getTestPath("gold"), 
        getTestPath("results")
      )
      
      // All should be under data/test
      testPaths.foreach { path =>
        path should startWith("data/test")
        path should not contain("data/output")
      }
    }
    
    "create isolated test artifacts" in {
      // Create a test file in the isolated area
      val testFile = s"${getTestPath("silver")}/isolation-test-file.txt"
      Files.write(Paths.get(testFile), "test content".getBytes)
      
      // Verify it exists in test area
      Files.exists(Paths.get(testFile)) should be(true)
      testFile should startWith("data/test")
      
      // Verify it's NOT in production area
      val productionEquivalent = testFile.replace("data/test", "data/output")
      Files.exists(Paths.get(productionEquivalent)) should be(false)
    }
  }
}

