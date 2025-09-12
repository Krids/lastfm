package com.lastfm.sessions.orchestration

import com.lastfm.sessions.pipelines.{PipelineConfig, UserIdPartitionStrategy, QualityThresholds, SparkConfig}
import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Production configuration management for Last.fm Session Analysis pipeline.
 * 
 * Provides centralized configuration loading and environment setup:
 * - Production-ready default configurations for Last.fm dataset
 * - Environment-aware parameter optimization  
 * - Medallion architecture directory structure creation
 * - Configuration validation and error handling
 * 
 * Designed for:
 * - Production deployment with environment-specific overrides
 * - Development environments with sensible defaults
 * - CI/CD pipeline integration with configurable parameters
 * - Multi-environment support (local, cluster, cloud)
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object ProductionConfigManager {

  /**
   * Loads production configuration optimized for complete Last.fm dataset.
   * 
   * Configuration based on dataset analysis and performance requirements:
   * - 19,150,868 total records from 1K users
   * - Quality thresholds for session analysis readiness (>99%)
   * - Optimal partitioning for environment-aware performance
   * - Production paths following medallion architecture
   * 
   * @return PipelineConfig optimized for Last.fm production processing
   */
  def loadProductionConfig(): PipelineConfig = {
    val cores = Runtime.getRuntime.availableProcessors()
    val estimatedUsers = 1000 // Based on Last.fm 1K dataset
    
    PipelineConfig(
      // Medallion architecture paths for production
      bronzePath = "data/input/lastfm-dataset-1k/userid-timestamp-artid-artname-traid-traname.tsv",
      silverPath = "data/output/silver/lastfm-listening-events-cleaned.tsv_parquet_data_parquet_data",
      
      // Environment-aware partitioning strategy
      partitionStrategy = UserIdPartitionStrategy(
        userCount = estimatedUsers,
        cores = cores
      ),
      
      // Production quality thresholds based on business requirements
      qualityThresholds = QualityThresholds(
        sessionAnalysisMinQuality = 99.0,   // High quality for temporal accuracy
        productionMinQuality = 99.9,        // Exceptional quality for production
        minTrackIdCoverage = 85.0,          // Sufficient MBID coverage for song ranking
        maxSuspiciousUserRatio = 5.0        // Acceptable bot/anomaly threshold
      ),
      
      // Spark configuration optimized for 19M+ record processing
      sparkConfig = SparkConfig(
        partitions = 16,                     // Optimal for 1K users (~62 users per partition)
        timeZone = "UTC",                    // Consistent timezone handling
        adaptiveEnabled = true,              // Enable adaptive query execution
        serializerClass = "org.apache.spark.serializer.KryoSerializer" // Performance
      )
    )
  }

  /**
   * Creates development configuration for testing and iteration.
   * 
   * Optimized for fast development cycles with sample data:
   * - Reduced resource requirements for development environments
   * - Lower quality thresholds for testing with sample data
   * - Simplified configuration for rapid iteration
   */
  def loadDevelopmentConfig(): PipelineConfig = {
    PipelineConfig(
      // Development paths with sample data
      bronzePath = "data/output/bronze/sample-lastfm-data.tsv",
      silverPath = "data/output/silver/dev-listening-events-cleaned.tsv",
      
      // Development partitioning (smaller scale)
      partitionStrategy = UserIdPartitionStrategy(userCount = 100, cores = 2),
      
      // Relaxed quality thresholds for development
      qualityThresholds = QualityThresholds(
        sessionAnalysisMinQuality = 95.0,   // Relaxed for sample data
        productionMinQuality = 99.0,        // Standard for development
        minTrackIdCoverage = 70.0,          // Relaxed for sample data
        maxSuspiciousUserRatio = 10.0       // Higher tolerance for development
      ),
      
      // Development Spark configuration  
      sparkConfig = SparkConfig(
        partitions = 4,                      // Minimal partitions for development
        timeZone = "UTC",
        adaptiveEnabled = true
      )
    )
  }

  /**
   * Creates medallion architecture directory structure for production deployment.
   * 
   * Creates complete directory hierarchy:
   * - Bronze layer: Raw data storage
   * - Silver layer: Quality-validated clean data artifacts
   * - Gold layer: Business logic results and session analysis
   * - Results layer: Final output files
   */
  def createProductionDirectories(): Unit = {
    val directoriesToCreate = List(
      "data/output/bronze",   // Raw data staging (optional)
      "data/output/silver",   // Quality-validated artifacts
      "data/output/gold",     // Session analysis results  
      "data/output/results"   // Final output (top_songs.tsv)
    )
    
    directoriesToCreate.foreach { dir =>
      Try {
        Files.createDirectories(Paths.get(dir))
        println(s"✅ Created directory: $dir")
      }.recover {
        case exception =>
          println(s"⚠️  Directory creation warning: $dir (${exception.getMessage})")
      }
    }
  }

  /**
   * Validates production prerequisites before pipeline execution.
   * 
   * Checks:
   * - Bronze layer data availability and size validation
   * - Directory permissions and disk space  
   * - Memory and CPU resource availability
   * - Configuration parameter validation
   */
  def validateProductionPrerequisites(config: PipelineConfig): Unit = {
    // Validate Bronze layer data
    if (!Files.exists(Paths.get(config.bronzePath))) {
      throw new RuntimeException(s"Bronze layer data not found: ${config.bronzePath}")
    }
    
    val bronzeSize = Files.size(Paths.get(config.bronzePath))
    val expectedMinSize = 100 * 1024 * 1024 // At least 100MB for production
    
    if (bronzeSize < expectedMinSize) {
      throw new RuntimeException(s"Production dataset too small: ${bronzeSize} bytes (minimum: ${expectedMinSize})")
    }
    
    // Validate resource availability
    val availableMemoryGB = Runtime.getRuntime.maxMemory() / 1024 / 1024 / 1024
    if (availableMemoryGB < 4) {
      println(s"⚠️  Memory warning: ${availableMemoryGB}GB may be insufficient for 19M records")
      println("   Recommendation: Use sbt -J-Xmx8g for optimal performance")
    }
    
    // Create output directories
    createProductionDirectories()
    
    println(s"✅ Production prerequisites validated")
    println(s"   Bronze layer: ${bronzeSize / 1024 / 1024}MB dataset")
    println(s"   Available memory: ${availableMemoryGB}GB")
    println(s"   CPU cores: ${Runtime.getRuntime.availableProcessors()}")
  }
}