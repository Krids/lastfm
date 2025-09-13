package com.lastfm.sessions.config

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.lastfm.sessions.utils.{BaseTestSpec, TestConfiguration}
import scala.util.{Try, Success, Failure}

/**
 * Test specification for AppConfiguration following clean architecture principles.
 * 
 * Tests configuration loading, validation, and path resolution while ensuring
 * complete isolation from production environment.
 * 
 * CRITICAL: This test class now uses BaseTestSpec to prevent production data
 * contamination. All configuration tests use TestConfiguration.testConfig().
 */
class AppConfigurationSpec extends AnyWordSpec with BaseTestSpec with Matchers {
  
  "AppConfiguration" should {
    
    "load test configuration values safely" in {
      // Use test-isolated configuration - NEVER AppConfiguration.default() in tests!
      val config = TestConfiguration.testConfig()
      
      config.sparkAppName should be("LastFM-Session-Analyzer")
      config.sparkMaster should be("local[2]")  // Using test config value
      config.sessionGapMinutes should be(20)
      config.topSessions should be(50)
      config.topTracks should be(10)
      config.isTest should be(true)
      config.environment should be("test")
    }
    
    "use configuration overrides while maintaining test isolation" in {
      val overrides = Map(
        "spark.app.name" -> "TestApp",
        "pipeline.session.gap.minutes" -> 30,
        "pipeline.ranking.top.sessions" -> 100
      )
      
      val config = TestConfiguration.testConfigWithOverrides(overrides)
      
      config.sparkAppName should be("TestApp")
      config.sessionGapMinutes should be(30)
      config.topSessions should be(100)
      // Ensure test isolation is maintained
      config.isTest should be(true)
      config.outputBasePath should startWith("data/test")
    }
    
    "provide test-isolated path configurations" in {
      val config = TestConfiguration.testConfig()
      
      // Input paths can use production data safely (read-only)
      config.inputBasePath should be("data/input")
      
      // Output paths must be test-isolated (write operations)
      config.outputBasePath should be("data/test")
      config.bronzePath should be("data/test/bronze")
      config.silverPath should be("data/test/silver")
      config.goldPath should be("data/test/gold")
      config.resultsPath should be("data/test/results")
    }
    
    "provide performance configurations in test environment" in {
      val config = TestConfiguration.testConfig()
      
      config.defaultPartitions should be(8)  // Using test config values
      config.rankingPartitions should be(4)  // Using test config values
      config.outputPartitions should be(1)
      config.isCacheEnabled should be(true)
      config.cacheStorageLevel should be("MEMORY_AND_DISK_SER")
    }
    
    "provide retry configurations in test environment" in {
      val config = TestConfiguration.testConfig()
      
      config.maxRetryAttempts should be(3)
      config.retryDelayMs should be(1000)
      config.retryBackoffMultiplier should be(2.0)
    }
    
    "provide monitoring configurations in test environment" in {
      val config = TestConfiguration.testConfig()
      
      config.isMetricsEnabled should be(true)
      config.metricsIntervalSeconds should be(60)
      config.performanceWarningThresholdMs should be(5000)
      config.performanceErrorThresholdMs should be(30000)
    }
    
    "provide quality configurations in test environment" in {
      val config = TestConfiguration.testConfig()
      
      config.qualityThreshold should be(95.0)
      config.maxRejectionRate should be(0.05)
    }
    
    "detect test environment correctly" in {
      val config = TestConfiguration.testConfig()
      
      config.environment should be("test")
      config.isProduction should be(false)
      config.isTest should be(true)
    }
    
    "create test-isolated PipelineConfig from configuration" in {
      val config = TestConfiguration.testConfig()
      val pipelineConfig = config.toPipelineConfig
      
      // Validate all paths are test-isolated
      pipelineConfig.outputPath should be(config.resultsPath)
      pipelineConfig.outputPath should startWith("data/test")
      pipelineConfig.bronzePath should startWith("data/test")
      pipelineConfig.silverPath should startWith("data/test")
      pipelineConfig.goldPath should startWith("data/test")
      pipelineConfig.sparkConfig.partitions should be(8)  // Test config value
      pipelineConfig.sparkConfig.adaptiveEnabled should be(true)
      pipelineConfig.qualityThresholds.sessionAnalysisMinQuality should be(config.qualityThreshold)
      pipelineConfig.qualityThresholds.maxSuspiciousUserRatio should be(config.maxRejectionRate * 100)
    }
    
    "validate test configuration successfully with valid values" in {
      val config = TestConfiguration.testConfig()
      
      config.validate() should be(Success(()))
    }
    
    "fail validation with invalid session gap in test environment" in {
      val overrides = Map("pipeline.session.gap.minutes" -> 0)
      val config = TestConfiguration.testConfigWithOverrides(overrides)
      
      config.validate() match {
        case Failure(e) => e.getMessage should include("Session gap must be positive")
        case _ => fail("Expected validation failure")
      }
    }
    
    "fail validation with invalid quality threshold in test environment" in {
      val overrides = Map("pipeline.quality.acceptable.threshold" -> 101.0)
      val config = TestConfiguration.testConfigWithOverrides(overrides)
      
      config.validate() match {
        case Failure(e) => e.getMessage should include("Quality threshold must be between 0 and 100")
        case _ => fail("Expected validation failure")
      }
    }
    
    "fail validation with invalid performance thresholds in test environment" in {
      val overrides = Map(
        "monitoring.performance.warning.threshold.ms" -> 10000,
        "monitoring.performance.error.threshold.ms" -> 5000
      )
      val config = TestConfiguration.testConfigWithOverrides(overrides)
      
      config.validate() match {
        case Failure(e) => e.getMessage should include("Performance error threshold must be greater than warning threshold")
        case _ => fail("Expected validation failure")
      }
    }
  }
  
  "AppConfiguration with mock port" should {
    
    "use values from configuration port" in {
      val mockPort = new MockConfigurationPort(Map(
        "spark.app.name" -> "MockApp",
        "pipeline.session.gap.minutes" -> 25,
        "environment" -> "test"
      ))
      
      val config = AppConfiguration.withPort(mockPort)
      
      config.sparkAppName should be("MockApp")
      config.sessionGapMinutes should be(25)
      config.environment should be("test")
      config.isTest should be(true)
    }
    
    "use default values when paths don't exist" in {
      val mockPort = new MockConfigurationPort(Map.empty)
      val config = AppConfiguration.withPort(mockPort)
      
      config.sparkAppName should be("LastFM-Session-Analyzer")
      config.sessionGapMinutes should be(20)
      config.topSessions should be(50)
    }
  }
}

/**
 * Mock implementation of ConfigurationPort for testing
 */
class MockConfigurationPort(values: Map[String, Any]) extends ConfigurationPort {
  
  override def getString(path: String): Try[String] = 
    values.get(path).map(v => Success(v.toString)).getOrElse(Failure(new NoSuchElementException(path)))
  
  override def getInt(path: String): Try[Int] = 
    values.get(path).map {
      case i: Int => Success(i)
      case v => Success(v.toString.toInt)
    }.getOrElse(Failure(new NoSuchElementException(path)))
  
  override def getBoolean(path: String): Try[Boolean] = 
    values.get(path).map {
      case b: Boolean => Success(b)
      case v => Success(v.toString.toBoolean)
    }.getOrElse(Failure(new NoSuchElementException(path)))
  
  override def getDouble(path: String): Try[Double] = 
    values.get(path).map {
      case d: Double => Success(d)
      case v => Success(v.toString.toDouble)
    }.getOrElse(Failure(new NoSuchElementException(path)))
  
  override def getStringOrDefault(path: String, default: String): String = 
    values.get(path).map(_.toString).getOrElse(default)
  
  override def getIntOrDefault(path: String, default: Int): Int = 
    values.get(path).map {
      case i: Int => i
      case v => v.toString.toInt
    }.getOrElse(default)
  
  override def getBooleanOrDefault(path: String, default: Boolean): Boolean = 
    values.get(path).map {
      case b: Boolean => b
      case v => v.toString.toBoolean
    }.getOrElse(default)
  
  override def getDoubleOrDefault(path: String, default: Double): Double = 
    values.get(path).map {
      case d: Double => d
      case v => v.toString.toDouble
    }.getOrElse(default)
  
  override def hasPath(path: String): Boolean = 
    values.contains(path)
  
  override def getEnvironment(): String = 
    getStringOrDefault("environment", "development")
}