package com.lastfm.sessions.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for Constants and ConfigurableConstants.
 * 
 * Simplified version focusing on core functionality validation.
 */
class ConstantsSpec extends AnyFlatSpec with Matchers {
  
  "Constants" should "provide sensible default values" in {
    Constants.Partitioning.DEFAULT_PARTITIONS should be >= 1
    Constants.SessionAnalysis.SESSION_GAP_MINUTES should be > 0
    Constants.SessionAnalysis.TOP_SESSIONS_COUNT should be > 0
    Constants.SessionAnalysis.TOP_TRACKS_COUNT should be > 0
    Constants.DataQuality.SESSION_ANALYSIS_MIN_QUALITY should be >= 0.0
    Constants.DataQuality.SESSION_ANALYSIS_MIN_QUALITY should be <= 100.0
  }
  
  it should "provide consistent business rules" in {
    Constants.SessionAnalysis.SESSION_GAP_SECONDS should be(Constants.SessionAnalysis.SESSION_GAP_MINUTES * 60)
    Constants.DataQuality.PRODUCTION_MIN_QUALITY should be >= Constants.DataQuality.SESSION_ANALYSIS_MIN_QUALITY
    Constants.Partitioning.MIN_PARTITIONS should be <= Constants.Partitioning.MAX_PARTITIONS
  }
  
  it should "provide valid file formats" in {
    Constants.FileFormats.TSV_EXTENSION should be(".tsv")
    Constants.FileFormats.PARQUET_EXTENSION should be(".parquet")
    Constants.FileFormats.TSV_DELIMITER should be("\t")
    Constants.FileFormats.TSV_HEADER_TRACKS should not be empty
  }
  
  it should "provide valid data patterns" in {
    Constants.DataPatterns.USER_ID_PATTERN should not be empty
    Constants.DataPatterns.USER_ID_PREFIX should be("user_")
    Constants.DataPatterns.TRACK_KEY_SEPARATOR should not be empty
  }
  
  "ConfigurableConstants" should "use sensible defaults" in {
    ConfigurableConstants.Partitioning.DEFAULT_PARTITIONS.value should be >= 1
    ConfigurableConstants.SessionAnalysis.SESSION_GAP_MINUTES.value should be > 0
    ConfigurableConstants.DataQuality.SESSION_ANALYSIS_MIN_QUALITY.value should be >= 0.0
    ConfigurableConstants.DataQuality.SESSION_ANALYSIS_MIN_QUALITY.value should be <= 100.0
  }
  
  it should "validate configuration correctly" in {
    // Valid configuration should pass
    ConfigurableConstants.validateConfiguration().isSuccess should be(true)
  }
  
  it should "provide configuration summary" in {
    val summary = ConfigurableConstants.getConfigurationSummary()
    
    summary should contain key "spark.partitions.default"
    summary should contain key "session.gap.minutes"
    summary should contain key "top.sessions"
    summary should contain key "top.tracks"
    
    // All values should be non-empty
    summary.values.foreach(_ should not be empty)
  }
}