package com.lastfm.sessions.pipelines

import com.lastfm.sessions.domain.DataQualityMetrics
import com.lastfm.sessions.utils.BaseTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession

/**
 * Simplified TDD specification for Data Cleaning Pipeline.
 * 
 * Focuses on core functionality testing without complex dependencies.
 * Uses proper test isolation to prevent production data contamination.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class DataCleaningSimpleSpec extends AnyFlatSpec with BaseTestSpec with Matchers {

  // Create test Spark session
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataCleaningPipeline-SimpleTest")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  "DataCleaningPipeline" should "calculate optimal partition count for session analysis" in {
    // Given: Pipeline with test configuration
    val testConfig = PipelineConfig(
      bronzePath = s"${getTestPath("bronze")}/test-input.tsv",
      silverPath = s"${getTestPath("silver")}/test-output",
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(99.0, 99.9, 85.0, 5.0),
      sparkConfig = SparkConfig(16, "UTC", true)
    )
    val pipeline = new DataCleaningPipeline(testConfig)
    
    // When: Optimal partitions are calculated
    val optimalPartitions = pipeline.calculateOptimalPartitions(1000)
    
    // Then: Partition count is optimized for session analysis
    optimalPartitions should be >= 16
    optimalPartitions should be <= 200
  }
  
  it should "validate partition balance metrics" in {
    // Given: Pipeline
    val testConfig = PipelineConfig(
      bronzePath = s"${getTestPath("bronze")}/test-input.tsv",
      silverPath = s"${getTestPath("silver")}/test-output",
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 100, cores = 2),
      qualityThresholds = QualityThresholds(95.0, 99.0, 80.0, 10.0),
      sparkConfig = SparkConfig(4, "UTC", true)
    )
    val pipeline = new DataCleaningPipeline(testConfig)
    
    // When: Partition balance is validated (with non-existent path)
    val balanceMetrics = pipeline.validatePartitionBalance("non-existent-path")
    
    // Then: Balance metrics are returned (even if path doesn't exist)
    balanceMetrics shouldBe a[PartitionBalanceMetrics]
    balanceMetrics.maxPartitionSkew should be >= 0.0
  }
  
  it should "provide partitioning strategy information" in {
    // Given: Pipeline
    val testConfig = PipelineConfig(
      bronzePath = s"${getTestPath("bronze")}/test-input.tsv",
      silverPath = s"${getTestPath("silver")}/test-output", 
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 500, cores = 8),
      qualityThresholds = QualityThresholds(99.0, 99.9, 85.0, 5.0),
      sparkConfig = SparkConfig(16, "UTC", true)
    )
    val pipeline = new DataCleaningPipeline(testConfig)
    
    // When: Partitioning strategy is requested
    val strategyInfo = pipeline.getPartitioningStrategy()
    
    // Then: Strategy information is provided
    strategyInfo.strategy shouldBe "userId"
    strategyInfo.eliminatesSessionAnalysisShuffle shouldBe true
    strategyInfo.optimalForSessionAnalysis shouldBe true
    strategyInfo.partitionCount should be > 0
  }
  
  it should "analyze memory usage metrics" in {
    // Given: Pipeline
    val testConfig = PipelineConfig(
      bronzePath = s"${getTestPath("bronze")}/test-input.tsv",
      silverPath = s"${getTestPath("silver")}/test-output",
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(99.0, 99.9, 85.0, 5.0),
      sparkConfig = SparkConfig(16, "UTC", true)
    )
    val pipeline = new DataCleaningPipeline(testConfig)
    
    // When: Memory usage is analyzed
    val memoryMetrics = pipeline.getMemoryUsageMetrics()
    
    // Then: Memory metrics are provided
    memoryMetrics.maxDriverMemoryMB should be > 0L
    memoryMetrics.usedDriverMemoryMB should be >= 0L
    memoryMetrics.memoryUtilizationPercent should be >= 0.0
    memoryMetrics.memoryUtilizationPercent should be <= 100.0
  }
  
  it should "handle partition skew analysis" in {
    // Given: Pipeline
    val testConfig = PipelineConfig(
      bronzePath = s"${getTestPath("bronze")}/test-input.tsv",
      silverPath = s"${getTestPath("silver")}/test-output",
      goldPath = getTestPath("gold"),
      outputPath = getTestPath("results"),
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = 4),
      qualityThresholds = QualityThresholds(99.0, 99.9, 85.0, 5.0),
      sparkConfig = SparkConfig(16, "UTC", true)
    )
    val pipeline = new DataCleaningPipeline(testConfig)
    
    // When: Partition skew is analyzed
    val skewMetrics = pipeline.analyzePartitionSkew("non-existent-path")
    
    // Then: Skew metrics are provided
    skewMetrics shouldBe a[PartitionSkewMetrics]
    skewMetrics.skewRatio should be >= 0.0
  }
}
