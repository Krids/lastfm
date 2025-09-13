package com.lastfm.sessions.config

import com.lastfm.sessions.pipelines.PipelineConfig
import scala.util.Try

/**
 * Application configuration service that provides domain-specific configuration access.
 * This class acts as a domain service that uses the ConfigurationPort to retrieve
 * configuration values without coupling to implementation details.
 */
class AppConfiguration(configPort: ConfigurationPort) {
  
  /**
   * Gets the Spark application name
   */
  def sparkAppName: String = 
    configPort.getStringOrDefault("spark.app.name", "LastFM-Session-Analyzer")
  
  /**
   * Gets the Spark master URL
   */
  def sparkMaster: String = 
    configPort.getStringOrDefault("spark.master", "local[*]")
  
  /**
   * Gets the default number of partitions
   */
  def defaultPartitions: Int = 
    configPort.getIntOrDefault("spark.partitions.default", 16)
  
  /**
   * Gets the number of partitions for ranking operations
   */
  def rankingPartitions: Int = 
    configPort.getIntOrDefault("spark.partitions.ranking", 8)
  
  /**
   * Gets the number of partitions for output files
   */
  def outputPartitions: Int = 
    configPort.getIntOrDefault("spark.partitions.output", 1)
  
  /**
   * Gets the Spark driver memory
   */
  def sparkDriverMemory: String = 
    configPort.getStringOrDefault("spark.driver.memory", "4g")
  
  /**
   * Gets the Spark executor memory
   */
  def sparkExecutorMemory: String = 
    configPort.getStringOrDefault("spark.executor.memory", "4g")
  
  /**
   * Gets the session gap in minutes
   */
  def sessionGapMinutes: Int = 
    configPort.getIntOrDefault("pipeline.session.gap.minutes", 20)
  
  /**
   * Gets the number of top sessions to analyze
   */
  def topSessions: Int = 
    configPort.getIntOrDefault("pipeline.ranking.top.sessions", 50)
  
  /**
   * Gets the number of top tracks to output
   */
  def topTracks: Int = 
    configPort.getIntOrDefault("pipeline.ranking.top.tracks", 10)
  
  /**
   * Gets the quality threshold percentage
   */
  def qualityThreshold: Double = 
    configPort.getDoubleOrDefault("pipeline.quality.acceptable.threshold", 95.0)
  
  /**
   * Gets the maximum rejection rate
   */
  def maxRejectionRate: Double = 
    configPort.getDoubleOrDefault("pipeline.quality.max.rejection.rate", 0.05)
  
  /**
   * Gets the maximum retry attempts
   */
  def maxRetryAttempts: Int = 
    configPort.getIntOrDefault("pipeline.retry.max.attempts", 3)
  
  /**
   * Gets the retry delay in milliseconds
   */
  def retryDelayMs: Long = 
    configPort.getIntOrDefault("pipeline.retry.delay.ms", 1000).toLong
  
  /**
   * Gets the retry backoff multiplier
   */
  def retryBackoffMultiplier: Double = 
    configPort.getDoubleOrDefault("pipeline.retry.backoff.multiplier", 2.0)
  
  /**
   * Checks if caching is enabled
   */
  def isCacheEnabled: Boolean = 
    configPort.getBooleanOrDefault("pipeline.cache.enabled", true)
  
  /**
   * Gets the cache storage level
   */
  def cacheStorageLevel: String = 
    configPort.getStringOrDefault("pipeline.cache.storage.level", "MEMORY_AND_DISK_SER")
  
  /**
   * Checks if metrics are enabled
   */
  def isMetricsEnabled: Boolean = 
    configPort.getBooleanOrDefault("monitoring.metrics.enabled", true)
  
  /**
   * Gets the metrics interval in seconds
   */
  def metricsIntervalSeconds: Int = 
    configPort.getIntOrDefault("monitoring.metrics.interval.seconds", 60)
  
  /**
   * Gets the performance warning threshold in milliseconds
   */
  def performanceWarningThresholdMs: Long = 
    configPort.getIntOrDefault("monitoring.performance.warning.threshold.ms", 5000).toLong
  
  /**
   * Gets the performance error threshold in milliseconds
   */
  def performanceErrorThresholdMs: Long = 
    configPort.getIntOrDefault("monitoring.performance.error.threshold.ms", 30000).toLong
  
  /**
   * Gets data input paths
   */
  def inputBasePath: String = 
    configPort.getStringOrDefault("data.input.base", "data/input")
  
  def inputDatasetPath: String = 
    configPort.getStringOrDefault("data.input.dataset", s"$inputBasePath/lastfm-dataset-1k")
  
  def inputProfilePath: String = 
    configPort.getStringOrDefault("data.input.profile", s"$inputDatasetPath/userid-profile.tsv")
  
  def inputEventsPath: String = 
    configPort.getStringOrDefault("data.input.events", s"$inputDatasetPath/userid-timestamp-artid-artname-traid-traname.tsv")
  
  /**
   * Gets data output paths
   */
  def outputBasePath: String = 
    configPort.getStringOrDefault("data.output.base", "data/output")
  
  def bronzePath: String = 
    configPort.getStringOrDefault("data.output.bronze", s"$outputBasePath/bronze")
  
  def silverPath: String = 
    configPort.getStringOrDefault("data.output.silver", s"$outputBasePath/silver")
  
  def goldPath: String = 
    configPort.getStringOrDefault("data.output.gold", s"$outputBasePath/gold")
  
  def resultsPath: String = 
    configPort.getStringOrDefault("data.output.results", s"$outputBasePath/results")
  
  /**
   * Gets the current environment
   */
  def environment: String = 
    configPort.getEnvironment()
  
  /**
   * Checks if running in production
   */
  def isProduction: Boolean = 
    environment.toLowerCase == "production"
  
  /**
   * Checks if running in test mode
   */
  def isTest: Boolean = 
    environment.toLowerCase == "test"
  
  /**
   * Creates a PipelineConfig from the current configuration
   */
  def toPipelineConfig: PipelineConfig = {
    import com.lastfm.sessions.pipelines._
    
    val partitionStrategy = UserIdPartitionStrategy(
      userCount = 1000, // Default for 1K dataset, could be made configurable
      cores = Runtime.getRuntime.availableProcessors()
    )
    
    val qualityThresholds = QualityThresholds(
      sessionAnalysisMinQuality = qualityThreshold,
      productionMinQuality = configPort.getDoubleOrDefault("pipeline.quality.production.threshold", 99.9),
      minTrackIdCoverage = configPort.getDoubleOrDefault("pipeline.quality.track.id.coverage", 85.0),
      maxSuspiciousUserRatio = maxRejectionRate * 100 // Convert from rate to percentage
    )
    
    val sparkConfig = SparkConfig(
      partitions = defaultPartitions,
      timeZone = configPort.getStringOrDefault("spark.sql.session.timeZone", "UTC"),
      adaptiveEnabled = configPort.getBooleanOrDefault("spark.sql.adaptive.enabled", true),
      serializerClass = configPort.getStringOrDefault("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    
    PipelineConfig(
      bronzePath = bronzePath,
      silverPath = silverPath,
      goldPath = goldPath,
      outputPath = resultsPath,
      partitionStrategy = partitionStrategy,
      qualityThresholds = qualityThresholds,
      sparkConfig = sparkConfig
    )
  }
  
  /**
   * Validates the configuration
   */
  def validate(): Try[Unit] = Try {
    require(sessionGapMinutes > 0, "Session gap must be positive")
    require(topSessions > 0, "Top sessions count must be positive")
    require(topTracks > 0, "Top tracks count must be positive")
    require(qualityThreshold >= 0 && qualityThreshold <= 100, "Quality threshold must be between 0 and 100")
    require(maxRejectionRate >= 0 && maxRejectionRate <= 1, "Max rejection rate must be between 0 and 1")
    require(maxRetryAttempts >= 0, "Max retry attempts must be non-negative")
    require(retryDelayMs >= 0, "Retry delay must be non-negative")
    require(retryBackoffMultiplier >= 1, "Retry backoff multiplier must be at least 1")
    require(defaultPartitions > 0, "Default partitions must be positive")
    require(rankingPartitions > 0, "Ranking partitions must be positive")
    require(outputPartitions > 0, "Output partitions must be positive")
    require(metricsIntervalSeconds > 0, "Metrics interval must be positive")
    require(performanceWarningThresholdMs > 0, "Performance warning threshold must be positive")
    require(performanceErrorThresholdMs > performanceWarningThresholdMs, 
            "Performance error threshold must be greater than warning threshold")
  }
}

/**
 * Factory for creating AppConfiguration instances
 */
object AppConfiguration {
  
  /**
   * Creates an AppConfiguration with the default Typesafe Config adapter
   */
  def default(): AppConfiguration = 
    new AppConfiguration(TypesafeConfigAdapter.forEnvironment())
  
  /**
   * Creates an AppConfiguration with a custom configuration port
   */
  def withPort(configPort: ConfigurationPort): AppConfiguration = 
    new AppConfiguration(configPort)
  
  /**
   * Creates an AppConfiguration with configuration overrides
   */
  def withOverrides(overrides: Map[String, Any]): AppConfiguration = 
    new AppConfiguration(TypesafeConfigAdapter.withOverrides(overrides))
}