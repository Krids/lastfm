package com.lastfm.config

/**
 * Clean, environment-aware configuration for the LastFM analysis pipeline.
 * 
 * Demonstrates configuration management best practices:
 * - Environment variable overrides for deployment flexibility
 * - Sensible defaults for local development
 * - Type-safe configuration with validation
 * - Clean separation of concerns
 * 
 * @author Felipe Lana Machado  
 * @since 1.0.0
 */
case class Config(
  // Business logic configuration
  sessionGapMinutes: Int,
  topSessionsCount: Int,
  topSongsCount: Int,
  
  // Data path configuration  
  inputPath: String,
  outputPath: String
) {
  // Validation to ensure configuration integrity
  require(sessionGapMinutes > 0, "Session gap must be positive")
  require(topSessionsCount > 0, "Top sessions count must be positive")
  require(topSongsCount > 0, "Top songs count must be positive")
  require(inputPath.nonEmpty, "Input path cannot be empty")
  require(outputPath.nonEmpty, "Output path cannot be empty")
}

object Config {
  
  /**
   * Creates configuration with environment variable overrides.
   * 
   * This pattern demonstrates production-ready configuration management:
   * - Environment variables allow runtime configuration without code changes
   * - Default values enable immediate local development
   * - Type conversion with fallback error handling
   */
  def load(): Config = {
    Config(
      sessionGapMinutes = getEnvInt("SESSION_GAP_MINUTES", 20),
      topSessionsCount = getEnvInt("TOP_SESSIONS", 50),
      topSongsCount = getEnvInt("TOP_SONGS", 10),
      inputPath = getEnvString("INPUT_PATH", "data/input/lastfm-dataset-1k"),
      outputPath = getEnvString("OUTPUT_PATH", "data/output")
    )
  }
  
  /**
   * Gets environment variable as integer with default fallback.
   */
  private def getEnvInt(envVar: String, default: Int): Int = {
    sys.env.get(envVar) match {
      case Some(value) => 
        try {
          value.toInt
        } catch {
          case _: NumberFormatException =>
            println(s"Warning: Invalid integer for $envVar='$value', using default: $default")
            default
        }
      case None => default
    }
  }
  
  /**
   * Gets environment variable as string with default fallback.
   */
  private def getEnvString(envVar: String, default: String): String = {
    sys.env.getOrElse(envVar, default)
  }
}
