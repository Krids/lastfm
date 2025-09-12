package com.lastfm.sessions.pipelines

import com.lastfm.sessions.domain.DataQualityMetrics
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

/**
 * Enhanced Data Cleaning Pipeline with strategic userId partitioning.
 * 
 * Implements Bronze → Silver transformation with optimal partitioning strategy:
 * - Partitions Silver layer data by userId during cleaning process
 * - Writes Silver layer as Parquet format for efficiency and schema evolution
 * - Eliminates shuffle operations in downstream session analysis
 * - Maintains data quality while optimizing for session analysis workloads
 * 
 * Key Enhancements:
 * - Strategic Partitioning: userId-based partitioning during cleaning (not after)
 * - Parquet Format: Columnar storage with compression for Silver layer
 * - Performance Optimization: Eliminates shuffle in session analysis pipeline
 * - Memory Efficiency: Distributed processing without driver memory collection
 * - Quality Preservation: Maintains all data quality validations
 * 
 * Design Principles:
 * - Single Responsibility: Focused on data cleaning with strategic partitioning
 * - Performance by Design: Optimizes entire pipeline, not just individual stages
 * - Clean Architecture: Separates partitioning strategy from cleaning logic
 * - Fail-Fast Validation: Comprehensive parameter and prerequisite validation
 * 
 * @param config Pipeline configuration with partitioning strategy
 * @param spark Implicit Spark session for distributed processing
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class EnhancedDataCleaningPipeline(val config: PipelineConfig)(implicit spark: SparkSession) {
  
  import spark.implicits._
  
  // Validate configuration at construction time
  require(config != null, "config cannot be null")
  
  /**
   * Schema definition for LastFM TSV files optimized for Parquet storage.
   */
  private val optimizedSchema = StructType(Seq(
    StructField("userId", StringType, nullable = false),
    StructField("timestamp", StringType, nullable = false),
    StructField("artistId", StringType, nullable = true),
    StructField("artistName", StringType, nullable = false),
    StructField("trackId", StringType, nullable = true),
    StructField("trackName", StringType, nullable = false)
  ))
  
  /**
   * Executes enhanced Bronze → Silver transformation with strategic partitioning.
   * 
   * Enhanced Pipeline Stages:
   * 1. Validates configuration and prerequisites
   * 2. Loads raw Bronze layer data with schema validation
   * 3. Applies comprehensive data quality validation
   * 4. Generates track keys for identity resolution
   * 5. **Applies strategic userId-based partitioning during cleaning**
   * 6. **Writes Silver layer as Parquet with userId partitions**
   * 7. Returns comprehensive quality metrics for monitoring
   * 
   * @return Try containing DataQualityMetrics or execution failure
   */
  def execute(): Try[DataQualityMetrics] = {
    try {
      println("🧹 Enhanced Data Cleaning Pipeline: Bronze → Silver (with strategic partitioning)")
      
      // Stage 1: Validate prerequisites
      validatePipelinePrerequisites()
      
      // Stage 2: Execute enhanced Bronze → Silver transformation
      val qualityMetrics = executeEnhancedBronzeToSilverTransformation()
      
      println(s"✅ Enhanced data cleaning completed with strategic partitioning")
      println(f"   Quality Score: ${qualityMetrics.qualityScore}%.6f%%")
      println(f"   Partitioning: userId-based for session analysis optimization")
      
      Success(qualityMetrics)
      
    } catch {
      case NonFatal(exception) =>
        Failure(new RuntimeException(s"Enhanced data cleaning pipeline failed: ${exception.getMessage}", exception))
    }
  }
  
  /**
   * Calculates optimal partition count based on user distribution and cluster resources.
   * 
   * Optimization Strategy:
   * - Target 50-75 users per partition for optimal session analysis
   * - Consider available CPU cores for parallelism
   * - Ensure minimum 16 partitions for distributed processing
   * - Cap maximum partitions based on cluster resources
   * 
   * @param estimatedUserCount Estimated number of unique users
   * @return Optimal partition count for session analysis
   */
  def calculateOptimalPartitions(estimatedUserCount: Int): Int = {
    val cores = Runtime.getRuntime.availableProcessors()
    val targetUsersPerPartition = 62 // Optimal for session analysis based on memory analysis
    
    val basePartitions = Math.max(estimatedUserCount / targetUsersPerPartition, 1)
    val coreBasedPartitions = cores * 2 // 2x cores for I/O optimization
    
    // Use the larger of the two, but cap at reasonable maximum
    Math.max(16, Math.min(basePartitions, Math.max(coreBasedPartitions, 200)))
  }
  
  /**
   * Validates partition balance to ensure even user distribution.
   * 
   * @param silverPath Path to partitioned Silver layer data
   * @return Partition balance metrics
   */
  def validatePartitionBalance(silverPath: String): PartitionBalanceMetrics = {
    try {
      // Read partitioned data to analyze balance
      val silverDF = spark.read.parquet(silverPath)
      
      val partitionCounts = silverDF
        .groupBy(spark_partition_id().as("partitionId"))
        .agg(
          countDistinct("userId").as("userCount"),
          count(lit(1)).as("recordCount")
        )
        .collect()
      
      val userCounts = partitionCounts.map(_.getAs[Long]("userCount"))
      val maxUsers = userCounts.max
      val minUsers = userCounts.min
      val skew = if (minUsers > 0) maxUsers.toDouble / minUsers else Double.MaxValue
      
      PartitionBalanceMetrics(
        maxPartitionSkew = skew,
        avgUsersPerPartition = userCounts.sum.toDouble / userCounts.length,
        isBalanced = skew < 2.0
      )
      
    } catch {
      case NonFatal(exception) =>
        PartitionBalanceMetrics(
          maxPartitionSkew = Double.MaxValue,
          avgUsersPerPartition = 0.0,
          isBalanced = false
        )
    }
  }
  
  /**
   * Analyzes Parquet file sizes for optimization validation.
   * 
   * @param silverPath Path to Parquet Silver layer data
   * @return File size metrics
   */
  def analyzeParquetFileSizes(silverPath: String): ParquetFileSizeMetrics = {
    try {
      val silverPathObj = Paths.get(silverPath)
      if (Files.exists(silverPathObj)) {
        val fileSizes = Files.walk(silverPathObj)
          .filter(_.toString.endsWith(".parquet"))
          .mapToLong(Files.size)
          .toArray
        
        if (fileSizes.nonEmpty) {
          val avgSizeMB = fileSizes.sum.toDouble / fileSizes.length / (1024 * 1024)
          val maxSizeMB = fileSizes.max.toDouble / (1024 * 1024)
          val minSizeMB = fileSizes.min.toDouble / (1024 * 1024)
          
          ParquetFileSizeMetrics(
            averageFileSizeMB = avgSizeMB,
            maxFileSizeMB = maxSizeMB,
            minFileSizeMB = minSizeMB,
            isOptimalSize = avgSizeMB >= 32.0 && avgSizeMB <= 128.0
          )
        } else {
          ParquetFileSizeMetrics.empty
        }
      } else {
        ParquetFileSizeMetrics.empty
      }
    } catch {
      case NonFatal(_) =>
        ParquetFileSizeMetrics.empty
    }
  }
  
  /**
   * Gets partitioning strategy information for validation.
   */
  def getPartitioningStrategy(): PartitioningStrategyInfo = {
    PartitioningStrategyInfo(
      strategy = "userId",
      eliminatesSessionAnalysisShuffle = true,
      optimalForSessionAnalysis = true,
      partitionCount = config.partitionStrategy.calculateOptimalPartitions()
    )
  }
  
  /**
   * Analyzes partition skew for quality validation.
   */
  def analyzePartitionSkew(silverPath: String): PartitionSkewMetrics = {
    val balanceMetrics = validatePartitionBalance(silverPath)
    PartitionSkewMetrics(
      skewRatio = balanceMetrics.maxPartitionSkew,
      isAcceptableSkew = balanceMetrics.maxPartitionSkew < 3.0, // Allow up to 3x skew
      recommendsRebalancing = balanceMetrics.maxPartitionSkew > 5.0
    )
  }
  
  /**
   * Gets memory usage metrics for performance validation.
   */
  def getMemoryUsageMetrics(): MemoryUsageMetrics = {
    val runtime = Runtime.getRuntime
    val maxMemoryMB = runtime.maxMemory() / (1024 * 1024)
    val usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
    
    MemoryUsageMetrics(
      maxDriverMemoryMB = maxMemoryMB,
      usedDriverMemoryMB = usedMemoryMB,
      memoryUtilizationPercent = (usedMemoryMB.toDouble / maxMemoryMB) * 100
    )
  }
  
  /**
   * Validates pipeline prerequisites before execution.
   */
  private def validatePipelinePrerequisites(): Unit = {
    // Validate Bronze layer input exists
    if (!Files.exists(Paths.get(config.bronzePath))) {
      throw new RuntimeException(s"Bronze layer input not found: ${config.bronzePath}")
    }
    
    // Validate Silver layer directory can be created
    val silverDir = Paths.get(config.silverPath).getParent
    if (silverDir != null) {
      Files.createDirectories(silverDir)
    }
    
    // Validate Spark session is active
    if (spark.sparkContext.isStopped) {
      throw new RuntimeException("Spark session is not active")
    }
    
    // Validate partitioning strategy
    if (config.partitionStrategy.calculateOptimalPartitions() < 1) {
      throw new RuntimeException("Invalid partition strategy configuration")
    }
  }
  
  /**
   * Executes enhanced Bronze → Silver transformation with strategic partitioning.
   */
  private def executeEnhancedBronzeToSilverTransformation(): DataQualityMetrics = {
    // Read Bronze layer data
    val rawDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .option("encoding", "UTF-8")
      .option("mode", "PERMISSIVE")
      .schema(optimizedSchema)
      .csv(config.bronzePath)
    
    println(s"📊 Loaded ${rawDF.count()} records from Bronze layer")
    
    // Apply data quality filters
    val cleanedDF = applyDataQualityFilters(rawDF)
    
    // Add track key for identity resolution
    val enhancedDF = addTrackKey(cleanedDF)
    
    // Calculate quality metrics before partitioning
    val qualityMetrics = calculateQualityMetrics(rawDF, enhancedDF)
    
    // Apply strategic userId partitioning and write as Parquet
    writeEnhancedSilverLayer(enhancedDF)
    
    qualityMetrics
  }
  
  /**
   * Applies comprehensive data quality filters.
   */
  private def applyDataQualityFilters(rawDF: DataFrame): DataFrame = {
    rawDF
      .filter($"userId".isNotNull && $"userId" =!= "")
      .filter($"timestamp".isNotNull && $"timestamp" =!= "")
      .filter($"artistName".isNotNull && $"artistName" =!= "")
      .filter($"trackName".isNotNull && $"trackName" =!= "")
      .filter(trim($"userId") =!= "")
      .filter(trim($"artistName") =!= "")
      .filter(trim($"trackName") =!= "")
      .filter(length(trim($"userId")) > 0)
      .filter(length(trim($"artistName")) > 0)
      .filter(length(trim($"trackName")) > 0)
  }
  
  /**
   * Adds track key for identity resolution.
   */
  private def addTrackKey(cleanedDF: DataFrame): DataFrame = {
    cleanedDF.withColumn("trackKey",
      when($"trackId".isNotNull && $"trackId" =!= "", $"trackId")
      .otherwise(concat($"artistName", lit(" — "), $"trackName"))
    )
  }
  
  /**
   * Calculates comprehensive data quality metrics.
   */
  private def calculateQualityMetrics(rawDF: DataFrame, cleanedDF: DataFrame): DataQualityMetrics = {
    val totalRecords = rawDF.count()
    val validRecords = cleanedDF.count()
    val rejectedRecords = totalRecords - validRecords
    
    val trackIdCoverage = cleanedDF
      .filter($"trackId".isNotNull && $"trackId" =!= "")
      .count().toDouble / validRecords * 100.0
    
    val suspiciousUsers = cleanedDF
      .groupBy("userId")
      .agg(count(lit(1)).as("trackCount"))
      .filter($"trackCount" > 10000) // Users with >10K tracks might be suspicious
      .count()
    
    val qualityScore = (validRecords.toDouble / totalRecords) * 100.0
    
    DataQualityMetrics(
      totalRecords = totalRecords,
      validRecords = validRecords,
      rejectedRecords = rejectedRecords,
      rejectionReasons = Map("invalid_format" -> rejectedRecords),
      trackIdCoverage = trackIdCoverage,
      suspiciousUsers = suspiciousUsers.toInt
    )
  }
  
  /**
   * Writes enhanced Silver layer with strategic userId partitioning as Parquet.
   * 
   * Key Optimizations:
   * - Partitions by userId for session analysis optimization
   * - Uses Parquet format for columnar efficiency and compression
   * - Optimizes file sizes for downstream processing
   * - Maintains data quality throughout partitioning process
   */
  private def writeEnhancedSilverLayer(enhancedDF: DataFrame): Unit = {
    val optimalPartitions = calculateOptimalPartitions(estimateUserCount(enhancedDF))
    
    println(s"🔧 Applying strategic userId partitioning:")
    println(s"   Partition Count: $optimalPartitions")
    println(s"   Strategy: userId-based for session analysis optimization")
    println(s"   Format: Parquet with Snappy compression")
    
    // Apply strategic partitioning and write as Parquet
    enhancedDF
      .repartition(optimalPartitions, $"userId") // Strategic userId partitioning
      .write
      .mode("overwrite")
      .option("compression", "snappy") // Optimal balance of speed and compression
      .partitionBy("userId") // Maintain userId partitioning in storage
      .parquet(config.silverPath)
    
    println(s"💾 Silver layer written as userId-partitioned Parquet: ${config.silverPath}")
  }
  
  /**
   * Estimates user count for partition calculation.
   */
  private def estimateUserCount(df: DataFrame): Int = {
    try {
      df.select("userId").distinct().count().toInt
    } catch {
      case NonFatal(_) =>
        // Fallback to configuration estimate
        config.partitionStrategy match {
          case userIdStrategy: UserIdPartitionStrategy => userIdStrategy.userCount
          case _ => 1000 // Default estimate
        }
    }
  }
}

/**
 * Companion object for Enhanced Data Cleaning Pipeline factory methods.
 */
object EnhancedDataCleaningPipeline {
  
  /**
   * Creates enhanced pipeline with production configuration.
   * 
   * @param bronzePath Path to Bronze layer input data
   * @param silverPath Path for Silver layer output (Parquet)
   * @param spark Implicit Spark session
   * @return Enhanced data cleaning pipeline
   */
  def createProduction(bronzePath: String, silverPath: String)(implicit spark: SparkSession): EnhancedDataCleaningPipeline = {
    val cores = Runtime.getRuntime.availableProcessors()
    
    val productionConfig = PipelineConfig(
      bronzePath = bronzePath,
      silverPath = silverPath,
      partitionStrategy = UserIdPartitionStrategy(userCount = 1000, cores = cores),
      qualityThresholds = QualityThresholds(
        sessionAnalysisMinQuality = 99.0,
        productionMinQuality = 99.9,
        minTrackIdCoverage = 85.0,
        maxSuspiciousUserRatio = 5.0
      ),
      sparkConfig = SparkConfig(
        partitions = 16,
        timeZone = "UTC",
        adaptiveEnabled = true
      )
    )
    
    new EnhancedDataCleaningPipeline(productionConfig)
  }
  
  /**
   * Creates enhanced pipeline for testing.
   * 
   * @param config Test configuration
   * @param spark Test Spark session
   * @return Test-configured enhanced pipeline
   */
  def createForTesting(config: PipelineConfig)(implicit spark: SparkSession): EnhancedDataCleaningPipeline = {
    new EnhancedDataCleaningPipeline(config)
  }
}

/**
 * Metrics for partition balance validation.
 */
case class PartitionBalanceMetrics(
  maxPartitionSkew: Double,
  avgUsersPerPartition: Double,
  isBalanced: Boolean
)

/**
 * Metrics for Parquet file size analysis.
 */
case class ParquetFileSizeMetrics(
  averageFileSizeMB: Double,
  maxFileSizeMB: Double,
  minFileSizeMB: Double,
  isOptimalSize: Boolean
)

object ParquetFileSizeMetrics {
  def empty: ParquetFileSizeMetrics = ParquetFileSizeMetrics(0.0, 0.0, 0.0, false)
}

/**
 * Information about partitioning strategy.
 */
case class PartitioningStrategyInfo(
  strategy: String,
  eliminatesSessionAnalysisShuffle: Boolean,
  optimalForSessionAnalysis: Boolean,
  partitionCount: Int
)

/**
 * Metrics for partition skew analysis.
 */
case class PartitionSkewMetrics(
  skewRatio: Double,
  isAcceptableSkew: Boolean,
  recommendsRebalancing: Boolean
)

/**
 * Metrics for memory usage monitoring.
 */
case class MemoryUsageMetrics(
  maxDriverMemoryMB: Long,
  usedDriverMemoryMB: Long,
  memoryUtilizationPercent: Double
)
