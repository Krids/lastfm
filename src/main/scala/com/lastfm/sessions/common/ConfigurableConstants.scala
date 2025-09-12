package com.lastfm.sessions.common

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.{Try, Success, Failure}

/**
 * Constants that can be overridden by configuration.
 * Provides a hierarchy: Default -> Config File -> Environment Variables
 * 
 * This allows runtime configuration changes without recompilation,
 * supporting different environments and deployment scenarios.
 * 
 * Precedence Order:
 * 1. Environment Variables (highest priority)
 * 2. Configuration Files (application.conf)
 * 3. Default Values (fallback)
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object ConfigurableConstants {
  
  private lazy val config: Config = ConfigFactory.load()
  
  /**
   * Trait for constants that can be overridden by configuration
   */
  trait Configurable[T] {
    def default: T
    def configPath: String
    def envVar: Option[String] = None
    
    /**
     * Gets the configured value following precedence order:
     * Environment Variable > Config File > Default
     */
    def value: T = {
      // Priority 1: Environment Variable
      envVar.flatMap(sys.env.get).flatMap(parseEnvVar).getOrElse {
        // Priority 2: Config File
        if (config.hasPath(configPath)) {
          Try(getConfigValue(config, configPath)).getOrElse(default)
        } else {
          // Priority 3: Default
          default
        }
      }
    }
    
    protected def parseEnvVar(s: String): Option[T]
    protected def getConfigValue(config: Config, path: String): T
  }
  
  /**
   * Configurable integer constant
   */
  case class ConfigurableInt(
    default: Int,
    configPath: String,
    override val envVar: Option[String] = None
  ) extends Configurable[Int] {
    
    protected def parseEnvVar(s: String): Option[Int] = {
      Try(s.toInt).toOption
    }
    
    protected def getConfigValue(config: Config, path: String): Int = {
      config.getInt(path)
    }
  }
  
  /**
   * Configurable double constant
   */
  case class ConfigurableDouble(
    default: Double,
    configPath: String,
    override val envVar: Option[String] = None
  ) extends Configurable[Double] {
    
    protected def parseEnvVar(s: String): Option[Double] = {
      Try(s.toDouble).toOption
    }
    
    protected def getConfigValue(config: Config, path: String): Double = {
      config.getDouble(path)
    }
  }
  
  /**
   * Configurable string constant
   */
  case class ConfigurableString(
    default: String,
    configPath: String,
    override val envVar: Option[String] = None
  ) extends Configurable[String] {
    
    protected def parseEnvVar(s: String): Option[String] = {
      Some(s)
    }
    
    protected def getConfigValue(config: Config, path: String): String = {
      config.getString(path)
    }
  }
  
  /**
   * Configurable boolean constant
   */
  case class ConfigurableBoolean(
    default: Boolean,
    configPath: String,
    override val envVar: Option[String] = None
  ) extends Configurable[Boolean] {
    
    protected def parseEnvVar(s: String): Option[Boolean] = {
      s.toLowerCase match {
        case "true" | "1" | "yes" | "on" => Some(true)
        case "false" | "0" | "no" | "off" => Some(false)
        case _ => None
      }
    }
    
    protected def getConfigValue(config: Config, path: String): Boolean = {
      config.getBoolean(path)
    }
  }
  
  // Configurable constants with override capability
  
  /**
   * Spark partitioning constants with runtime configuration
   */
  object Partitioning {
    val DEFAULT_PARTITIONS = ConfigurableInt(
      default = Constants.Partitioning.DEFAULT_PARTITIONS,
      configPath = "spark.partitions.default",
      envVar = Some("SPARK_DEFAULT_PARTITIONS")
    )
    
    val RANKING_PARTITIONS = ConfigurableInt(
      default = Constants.Partitioning.RANKING_PARTITIONS,
      configPath = "spark.partitions.ranking",
      envVar = Some("SPARK_RANKING_PARTITIONS")
    )
    
    val USERS_PER_PARTITION_TARGET = ConfigurableInt(
      default = Constants.Partitioning.USERS_PER_PARTITION_TARGET,
      configPath = "spark.partitions.users-per-partition",
      envVar = Some("USERS_PER_PARTITION")
    )
    
    val MIN_PARTITIONS = ConfigurableInt(
      default = Constants.Partitioning.MIN_PARTITIONS,
      configPath = "spark.partitions.min",
      envVar = Some("SPARK_MIN_PARTITIONS")
    )
    
    val MAX_PARTITIONS = ConfigurableInt(
      default = Constants.Partitioning.MAX_PARTITIONS,
      configPath = "spark.partitions.max",
      envVar = Some("SPARK_MAX_PARTITIONS")
    )
  }
  
  /**
   * Session analysis constants with runtime configuration
   */
  object SessionAnalysis {
    val SESSION_GAP_MINUTES = ConfigurableInt(
      default = Constants.SessionAnalysis.SESSION_GAP_MINUTES,
      configPath = "pipeline.session.gap.minutes",
      envVar = Some("SESSION_GAP_MINUTES")
    )
    
    val TOP_SESSIONS_COUNT = ConfigurableInt(
      default = Constants.SessionAnalysis.TOP_SESSIONS_COUNT,
      configPath = "pipeline.ranking.top.sessions",
      envVar = Some("TOP_SESSIONS")
    )
    
    val TOP_TRACKS_COUNT = ConfigurableInt(
      default = Constants.SessionAnalysis.TOP_TRACKS_COUNT,
      configPath = "pipeline.ranking.top.tracks",
      envVar = Some("TOP_TRACKS")
    )
  }
  
  /**
   * Data quality constants with runtime configuration
   */
  object DataQuality {
    val SESSION_ANALYSIS_MIN_QUALITY = ConfigurableDouble(
      default = Constants.DataQuality.SESSION_ANALYSIS_MIN_QUALITY,
      configPath = "pipeline.quality.session-analysis-threshold",
      envVar = Some("SESSION_QUALITY_THRESHOLD")
    )
    
    val PRODUCTION_MIN_QUALITY = ConfigurableDouble(
      default = Constants.DataQuality.PRODUCTION_MIN_QUALITY,
      configPath = "pipeline.quality.production-threshold",
      envVar = Some("PRODUCTION_QUALITY_THRESHOLD")
    )
    
    val MIN_TRACK_ID_COVERAGE = ConfigurableDouble(
      default = Constants.DataQuality.MIN_TRACK_ID_COVERAGE,
      configPath = "pipeline.quality.min-track-coverage",
      envVar = Some("MIN_TRACK_COVERAGE")
    )
    
    val MAX_REJECTION_RATE = ConfigurableDouble(
      default = Constants.DataQuality.MAX_REJECTION_RATE,
      configPath = "pipeline.quality.max.rejection.rate",
      envVar = Some("MAX_REJECTION_RATE")
    )
  }
  
  /**
   * Performance monitoring constants with runtime configuration
   */
  object Performance {
    val WARNING_THRESHOLD_MS = ConfigurableInt(
      default = Constants.Performance.WARNING_THRESHOLD_MS.toInt,
      configPath = "monitoring.performance.warning.threshold.ms",
      envVar = Some("PERF_WARNING_MS")
    ).value.toLong
    
    val ERROR_THRESHOLD_MS = ConfigurableInt(
      default = Constants.Performance.ERROR_THRESHOLD_MS.toInt,
      configPath = "monitoring.performance.error.threshold.ms",
      envVar = Some("PERF_ERROR_MS")
    ).value.toLong
    
    val RETRY_MAX_ATTEMPTS = ConfigurableInt(
      default = Constants.Performance.RETRY_MAX_ATTEMPTS,
      configPath = "pipeline.retry.max.attempts",
      envVar = Some("RETRY_MAX_ATTEMPTS")
    )
    
    val RETRY_INITIAL_DELAY_MS = ConfigurableInt(
      default = Constants.Performance.RETRY_INITIAL_DELAY_MS.toInt,
      configPath = "pipeline.retry.delay.ms",
      envVar = Some("RETRY_DELAY_MS")
    ).value.toLong
    
    val RETRY_BACKOFF_MULTIPLIER = ConfigurableDouble(
      default = Constants.Performance.RETRY_BACKOFF_MULTIPLIER,
      configPath = "pipeline.retry.backoff.multiplier",
      envVar = Some("RETRY_BACKOFF_MULTIPLIER")
    )
  }
  
  /**
   * File paths with runtime configuration
   */
  object FilePaths {
    val DATA_BASE = ConfigurableString(
      default = Constants.FilePaths.DATA_BASE,
      configPath = "data.base",
      envVar = Some("DATA_BASE_PATH")
    )
    
    val BRONZE_LAYER = ConfigurableString(
      default = Constants.FilePaths.BRONZE_LAYER,
      configPath = "data.output.bronze",
      envVar = Some("BRONZE_LAYER_PATH")
    )
    
    val SILVER_LAYER = ConfigurableString(
      default = Constants.FilePaths.SILVER_LAYER,
      configPath = "data.output.silver",
      envVar = Some("SILVER_LAYER_PATH")
    )
    
    val GOLD_LAYER = ConfigurableString(
      default = Constants.FilePaths.GOLD_LAYER,
      configPath = "data.output.gold",
      envVar = Some("GOLD_LAYER_PATH")
    )
    
    val RESULTS_LAYER = ConfigurableString(
      default = Constants.FilePaths.RESULTS_LAYER,
      configPath = "data.output.results",
      envVar = Some("RESULTS_LAYER_PATH")
    )
  }
  
  /**
   * Cache configuration with runtime overrides
   */
  object Cache {
    val ENABLED = ConfigurableBoolean(
      default = true,
      configPath = "pipeline.cache.enabled",
      envVar = Some("CACHE_ENABLED")
    )
    
    val STORAGE_LEVEL = ConfigurableString(
      default = Constants.Performance.CACHE_STORAGE_LEVEL,
      configPath = "pipeline.cache.storage.level",
      envVar = Some("CACHE_STORAGE_LEVEL")
    )
  }
  
  /**
   * Utility method to validate all configuration values at startup
   */
  def validateConfiguration(): Try[Unit] = {
    Try {
      // Validate critical configurations
      require(Partitioning.DEFAULT_PARTITIONS.value > 0, "Default partitions must be positive")
      require(SessionAnalysis.SESSION_GAP_MINUTES.value > 0, "Session gap must be positive")
      require(SessionAnalysis.SESSION_GAP_MINUTES.value <= Constants.Limits.MAX_SESSION_DURATION_MINUTES, 
        s"Session gap cannot exceed ${Constants.Limits.MAX_SESSION_DURATION_MINUTES} minutes")
      require(DataQuality.SESSION_ANALYSIS_MIN_QUALITY.value >= 0 && 
        DataQuality.SESSION_ANALYSIS_MIN_QUALITY.value <= 100, 
        "Quality threshold must be between 0 and 100")
      require(Performance.RETRY_MAX_ATTEMPTS.value >= 0, "Max retry attempts cannot be negative")
    }
  }
  
  /**
   * Get current configuration summary for diagnostics
   */
  def getConfigurationSummary(): Map[String, String] = {
    Map(
      "spark.partitions.default" -> Partitioning.DEFAULT_PARTITIONS.value.toString,
      "session.gap.minutes" -> SessionAnalysis.SESSION_GAP_MINUTES.value.toString,
      "top.sessions" -> SessionAnalysis.TOP_SESSIONS_COUNT.value.toString,
      "top.tracks" -> SessionAnalysis.TOP_TRACKS_COUNT.value.toString,
      "quality.threshold" -> DataQuality.SESSION_ANALYSIS_MIN_QUALITY.value.toString,
      "cache.enabled" -> Cache.ENABLED.value.toString,
      "data.base" -> FilePaths.DATA_BASE.value,
      "retry.max.attempts" -> Performance.RETRY_MAX_ATTEMPTS.value.toString
    )
  }
}