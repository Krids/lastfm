package com.lastfm.sessions.common

/**
 * Central repository for all application constants.
 * Organized by domain context for easy discovery and maintenance.
 * 
 * Design Principles:
 * - Single source of truth for all magic numbers
 * - Organized by domain context for easy discovery
 * - Immutable constants with clear naming
 * - Documentation for business context
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object Constants {
  
  /**
   * Spark partitioning and performance constants
   */
  object Partitioning {
    /** Default number of partitions for balanced processing */
    val DEFAULT_PARTITIONS = 16
    
    /** Partitions for ranking operations (smaller datasets) */
    val RANKING_PARTITIONS = 8
    
    /** Single partition for output files (TSV generation) */
    val OUTPUT_PARTITIONS = 1
    
    /** Target users per partition for optimal session analysis */
    val USERS_PER_PARTITION_TARGET = 62
    
    /** Minimum partitions to maintain parallelism */
    val MIN_PARTITIONS = 16
    
    /** Maximum partitions for cluster environments */
    val MAX_PARTITIONS = 200
    
    /** Multiplier for core-based partition calculation */
    val PARTITION_MULTIPLIER = 2
  }
  
  /**
   * Session analysis business rules
   */
  object SessionAnalysis {
    /** Session gap in minutes - tracks within this time belong to same session */
    val SESSION_GAP_MINUTES = 20
    
    /** Session gap in seconds for calculations */
    val SESSION_GAP_SECONDS = 1200  // 20 * 60
    
    /** Number of top sessions to analyze for ranking */
    val TOP_SESSIONS_COUNT = 50
    
    /** Number of top tracks to output in final results */
    val TOP_TRACKS_COUNT = 10
  }
  
  /**
   * Data quality thresholds and business rules
   */
  object DataQuality {
    /** Minimum quality threshold for general acceptance */
    val ACCEPTABLE_QUALITY_THRESHOLD = 95.0
    
    /** Minimum quality required for session analysis */
    val SESSION_ANALYSIS_MIN_QUALITY = 99.0
    
    /** Minimum quality required for production deployment */
    val PRODUCTION_MIN_QUALITY = 99.9
    
    /** Minimum track ID coverage percentage */
    val MIN_TRACK_ID_COVERAGE = 85.0
    
    /** Maximum allowed rejection rate */
    val MAX_REJECTION_RATE = 0.05
    
    /** Maximum ratio of suspicious users */
    val MAX_SUSPICIOUS_USER_RATIO = 5.0
    
    /** Play count threshold for suspicious user detection */
    val SUSPICIOUS_USER_PLAY_THRESHOLD = 100000L
  }
  
  /**
   * File format constants and patterns
   */
  object FileFormats {
    /** Tab-separated values file extension */
    val TSV_EXTENSION = ".tsv"
    
    /** Parquet file extension */
    val PARQUET_EXTENSION = ".parquet"
    
    /** JSON file extension */
    val JSON_EXTENSION = ".json"
    
    /** TSV field delimiter */
    val TSV_DELIMITER = "\t"
    
    /** Standard TSV header for top tracks output */
    val TSV_HEADER_TRACKS = "rank\ttrack_name\tartist_name\tplay_count"
  }
  
  /**
   * Data patterns and formats
   */
  object DataPatterns {
    /** User ID regex pattern - must follow user_XXXXXX format */
    val USER_ID_PATTERN = "^user_\\d{6}$"
    
    /** User ID prefix */
    val USER_ID_PREFIX = "user_"
    
    /** MBID UUID pattern for validation */
    val MBID_UUID_PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    
    /** Track key separator for composite identifiers */
    val TRACK_KEY_SEPARATOR = " — "
  }
  
  /**
   * Performance monitoring and system limits
   */
  object Performance {
    /** Default cache storage level */
    val CACHE_STORAGE_LEVEL = "MEMORY_AND_DISK_SER"
    
    /** Warning threshold for slow operations (milliseconds) */
    val WARNING_THRESHOLD_MS = 5000L
    
    /** Error threshold for very slow operations (milliseconds) */
    val ERROR_THRESHOLD_MS = 30000L
    
    /** Maximum retry attempts for failed operations */
    val RETRY_MAX_ATTEMPTS = 3
    
    /** Initial delay between retries (milliseconds) */
    val RETRY_INITIAL_DELAY_MS = 1000L
    
    /** Backoff multiplier for exponential retry */
    val RETRY_BACKOFF_MULTIPLIER = 2.0
  }
  
  /**
   * File system paths and directory structure
   */
  object FilePaths {
    /** Base data directory */
    val DATA_BASE = "data"
    
    /** Bronze layer path for raw input */
    val BRONZE_LAYER = s"$DATA_BASE/output/bronze"
    
    /** Silver layer path for cleaned data */
    val SILVER_LAYER = s"$DATA_BASE/output/silver"
    
    /** Gold layer path for analyzed data */
    val GOLD_LAYER = s"$DATA_BASE/output/gold"
    
    /** Results layer path for final outputs */
    val RESULTS_LAYER = s"$DATA_BASE/output/results"
    
    /** Final output filename */
    val TOP_SONGS_FILENAME = "top_songs.tsv"
  }
  
  /**
   * Field length limits and validation bounds
   */
  object Limits {
    /** Maximum track name length */
    val MAX_TRACK_NAME_LENGTH = 500
    
    /** Maximum artist name length */
    val MAX_ARTIST_NAME_LENGTH = 500
    
    /** Minimum user ID length (user_ + 6 digits) */
    val MIN_USER_ID_LENGTH = 10
    
    /** Maximum user ID length for validation */
    val MAX_USER_ID_LENGTH = 50
    
    /** Maximum session duration in minutes (24 hours) */
    val MAX_SESSION_DURATION_MINUTES = 1440
  }

  /**
   * Utility methods for runtime environment detection
   */
  object Environment {
    
    /**
     * Detects if current execution is in test environment.
     * 
     * Uses comprehensive detection combining multiple indicators:
     * - Environment variables (ENV, SBT_TEST)
     * - JVM system properties (sbt.testing, java.class.path)
     * - Stack trace analysis for test frameworks
     * - SBT main class detection
     * 
     * @return true if running in test environment, false otherwise
     */
    def isTestEnvironment: Boolean = {
      // Check environment variables
      val envTest = sys.env.get("ENV").exists(_.toLowerCase.contains("test")) ||
                    sys.env.get("SBT_TEST").isDefined ||
                    sys.props.get("environment").exists(_.toLowerCase.contains("test"))
      
      // Check JVM properties set by test frameworks
      val jvmTest = sys.props.get("sbt.testing").isDefined ||
                    sys.props.get("java.class.path").exists(_.contains("scalatest")) ||
                    sys.props.get("sbt.main.class").exists(_.contains("sbt."))
      
      // Check for test framework in stack trace
      val stackTest = Thread.currentThread().getStackTrace
        .exists(frame => 
          frame.getClassName.contains("scalatest") || 
          frame.getClassName.contains("junit") ||
          frame.getClassName.contains("Spec") ||
          frame.getClassName.contains("Test")
        )
      
      envTest || jvmTest || stackTest
    }
  }
}

