package com.lastfm.sessions.testutil

import com.lastfm.sessions.config.AppConfiguration
import scala.util.{Try, Success, Failure}

/**
 * Centralized test configuration management following clean architecture principles.
 * 
 * Ensures ALL tests use isolated test paths and proper environment separation.
 * This is a critical safety mechanism to prevent test contamination of production data.
 * 
 * Design Principles:
 * - Fail-fast validation to catch production path usage immediately
 * - Clear separation between test and production configuration
 * - Immutable configuration objects to prevent runtime modification
 * - Comprehensive validation with descriptive error messages
 * 
 * Usage:
 * ```scala
 * // CORRECT - Use this in all tests
 * implicit val config = TestConfiguration.testConfig()
 * 
 * // WRONG - NEVER do this in tests
 * val config = AppConfiguration.default() // ❌ Uses production paths!
 * ```
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object TestConfiguration {
  
  /**
   * Creates test-specific configuration with isolated paths.
   * 
   * This method ensures all tests use data/test/ directory instead of data/output/
   * and validates the configuration before returning it.
   * 
   * @return Validated test configuration with isolated paths
   * @throws IllegalStateException if configuration validation fails
   */
  def testConfig(): AppConfiguration = {
    val testOverrides = Map(
      "data.output.base" -> "data/test",
      "environment" -> "test"
    )
    
    val config = AppConfiguration.withOverrides(testOverrides)
    
    // Validate configuration before returning
    validateTestIsolation(config) match {
      case Success(_) => config
      case Failure(exception) => 
        throw new IllegalStateException(
          s"Test configuration validation failed: ${exception.getMessage}", 
          exception
        )
    }
  }
  
  /**
   * Creates test configuration with custom overrides for specific test scenarios.
   * 
   * Useful for testing edge cases or specific configuration values while
   * maintaining test isolation.
   * 
   * @param additionalOverrides Custom configuration overrides
   * @return Validated test configuration with custom overrides
   */
  def testConfigWithOverrides(additionalOverrides: Map[String, Any]): AppConfiguration = {
    val baseTestOverrides = Map(
      "data.output.base" -> "data/test",
      "environment" -> "test"
    )
    
    val allOverrides = baseTestOverrides ++ additionalOverrides
    val config = AppConfiguration.withOverrides(allOverrides)
    
    validateTestIsolation(config) match {
      case Success(_) => config
      case Failure(exception) => 
        throw new IllegalStateException(
          s"Test configuration validation failed: ${exception.getMessage}", 
          exception
        )
    }
  }
  
  /**
   * Validates that configuration uses test-isolated paths and environment.
   * 
   * This is a critical safety check that prevents tests from accidentally
   * writing to production directories.
   * 
   * @param config Configuration to validate
   * @return Success if valid, Failure with detailed error if invalid
   */
  def validateTestIsolation(config: AppConfiguration): Try[Unit] = Try {
    // Critical path validation
    require(config.isTest, 
      s"CRITICAL SAFETY VIOLATION: Test configuration must have environment='test', got: ${config.environment}")
    
    require(config.outputBasePath.startsWith("data/test"), 
      s"CRITICAL SAFETY VIOLATION: Test must use data/test/ paths, got: ${config.outputBasePath}")
    
    require(!config.outputBasePath.contains("data/output"), 
      s"CRITICAL SAFETY VIOLATION: Test cannot use production path: ${config.outputBasePath}")
    
    // Validate all output paths are test-isolated
    val outputPaths = List(
      ("bronze", config.bronzePath),
      ("silver", config.silverPath), 
      ("gold", config.goldPath),
      ("results", config.resultsPath)
    )
    
    outputPaths.foreach { case (layerName, path) =>
      require(path.startsWith("data/test"), 
        s"CRITICAL: $layerName layer path must use data/test/, got: $path")
    }
    
    // Validate production cannot be accidentally accessed
    require(!config.isProduction, 
      "CRITICAL: Test configuration cannot be in production environment")
  }
  
  /**
   * Performs comprehensive pre-test validation to catch configuration issues early.
   * 
   * This method should be called in test setup to ensure the environment
   * is properly isolated before any test execution.
   * 
   * @param config Test configuration to validate
   * @throws IllegalStateException if any validation fails
   */
  def validateTestEnvironment(config: AppConfiguration): Unit = {
    validateTestIsolation(config) match {
      case Success(_) => 
        println(s"✅ Test environment validated - isolated to: ${config.outputBasePath}")
      case Failure(exception) =>
        System.err.println(s"🚨 CRITICAL TEST SAFETY VIOLATION: ${exception.getMessage}")
        throw new IllegalStateException(
          "Test environment validation failed - preventing potential production data contamination",
          exception
        )
    }
  }
  
  /**
   * Utility method to detect if current runtime is using production configuration.
   * 
   * Can be used in CI/CD pipelines or development environments to prevent
   * accidental production configuration usage in tests.
   * 
   * @return true if production configuration is detected, false if safe
   */
  def isProductionConfigurationDetected(): Boolean = {
    Try {
      val config = AppConfiguration.default()
      config.outputBasePath.contains("data/output") || config.isProduction
    }.getOrElse(false)
  }
  
  /**
   * Emergency safety check that can be called from anywhere to validate
   * the current configuration state.
   * 
   * @throws IllegalStateException if production contamination risk is detected
   */
  def emergencySafetyCheck(): Unit = {
    if (isProductionConfigurationDetected()) {
      throw new IllegalStateException(
        "🚨 EMERGENCY SAFETY VIOLATION: Production configuration detected in test context! " +
        "This could lead to data contamination. Use TestConfiguration.testConfig() instead."
      )
    }
  }
}

