package com.lastfm.sessions.orchestration

import com.lastfm.sessions.pipelines.{PipelineConfig, DistributedSessionAnalysisPipeline}
import com.lastfm.sessions.application.{DataCleaningServiceFactory, DistributedSessionAnalysisFactory}
import com.lastfm.sessions.domain.DataQualityMetrics
import org.apache.spark.sql.SparkSession
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

/**
 * Production pipeline orchestrator for Last.fm Session Analysis.
 * 
 * Manages execution of specialized pipeline contexts based on command-line arguments:
 * - data-cleaning: Bronze → Silver data quality transformation
 * - session-analysis: Silver → Gold session calculation (future implementation)
 * - ranking: Gold → Results top songs ranking (future implementation)
 * - complete: Full pipeline execution with dependency management
 * 
 * Implements enterprise orchestration patterns:
 * - Single entry point for all pipeline operations
 * - Pipeline dependency validation and execution ordering
 * - Comprehensive error handling with user-friendly messages
 * - Resource management and cleanup
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object PipelineOrchestrator {

  /**
   * Parses command-line arguments and executes appropriate pipeline.
   * 
   * @param args Command-line arguments specifying pipeline to execute
   * @param config Pipeline configuration for execution
   * @return PipelineExecutionResult indicating success or failure type
   */
  def parseArgsAndExecute(args: Array[String], config: PipelineConfig): PipelineExecutionResult = {
    try {
      args.headOption.getOrElse("complete") match {
        case "data-cleaning" => executeDataCleaningPipeline(config)
        case "session-analysis" => executeSessionAnalysisPipeline(config)  
        case "ranking" => executeRankingPipeline(config)
        case "complete" => executeCompletePipeline(config)
        case invalidArg => 
          displayUsageHelp()
          PipelineExecutionResult.InvalidArguments
      }
    } catch {
      case NonFatal(exception) =>
        println(s"❌ Pipeline execution failed: ${exception.getMessage}")
        exception.printStackTrace()
        PipelineExecutionResult.ExecutionFailed
    }
  }

  /**
   * Executes Data Cleaning Service (Bronze → Silver transformation).
   */
  private def executeDataCleaningPipeline(config: PipelineConfig): PipelineExecutionResult = {
    println("🧹 Executing Data Cleaning Service (Bronze → Silver)")
    
    using(SparkSessionManager.createProductionSession()) { implicit spark =>
      val service = DataCleaningServiceFactory.createProductionService
      val result = service.cleanData(config.bronzePath, config.silverPath)
      
      result match {
        case Success(qualityMetrics: DataQualityMetrics) =>
          println(s"✅ Data cleaning completed successfully")
          println(f"   Quality Score: ${qualityMetrics.qualityScore}%.6f%%")
          println(s"   Records: ${qualityMetrics.totalRecords} → ${qualityMetrics.validRecords}")
          println(s"   Format: Parquet with optimal userId partitioning")
          PipelineExecutionResult.DataCleaningCompleted
          
        case Failure(exception) =>
          throw new RuntimeException("Data cleaning service failed", exception)
      }
    }
  }

  /**
   * Executes Distributed Session Analysis Service (Silver → Gold transformation).
   * 
   * Implements memory-efficient distributed session analysis processing:
   * - Loads cleaned listening events from Silver layer as distributed streams
   * - Applies 20-minute gap algorithm using Spark window functions
   * - Generates comprehensive analysis without driver memory constraints
   * - Persists structured Gold layer artifacts for downstream consumption
   * 
   * Key Improvements:
   * - No OutOfMemoryError issues with large datasets (19M+ records)
   * - Distributed processing using Spark DataFrames throughout
   * - Window functions for efficient session boundary detection
   * - Single-pass aggregations for metrics calculation
   */
  private def executeSessionAnalysisPipeline(config: PipelineConfig): PipelineExecutionResult = {
    println("🔄 Executing Distributed Session Analysis Service (Silver → Gold)")
    
    using(SparkSessionManager.createProductionSession()) { implicit spark =>
      val service = DistributedSessionAnalysisFactory.createProductionService
      val result = service.analyzeUserSessions(config.silverPath, "data/output/gold")
      
      result match {
        case Success(analysis) =>
          println(s"✅ Distributed session analysis completed successfully")
          println(f"   Sessions Generated: ${analysis.metrics.totalSessions}")
          println(f"   Users Analyzed: ${analysis.metrics.uniqueUsers}")
          println(f"   Quality Score: ${analysis.metrics.qualityScore}%.2f%%")
          println(f"   Quality Assessment: ${analysis.qualityAssessment}")
          println(f"   Performance Category: ${analysis.performanceCategory}")
          PipelineExecutionResult.SessionAnalysisCompleted
          
        case Failure(exception) =>
          throw new RuntimeException("Distributed session analysis service failed", exception)
      }
    }
  }

  /**
   * Executes Ranking Pipeline (Gold → Results transformation).
   * Future implementation - currently returns placeholder.
   */
  private def executeRankingPipeline(config: PipelineConfig): PipelineExecutionResult = {
    println("🏆 Ranking Pipeline - Not yet implemented")
    println("   Next: Top 50 longest sessions → Top 10 songs ranking")
    PipelineExecutionResult.RankingCompleted
  }

  /**
   * Executes complete pipeline with proper dependency management.
   */
  private def executeCompletePipeline(config: PipelineConfig): PipelineExecutionResult = {
    println("🚀 Executing Complete Pipeline (Bronze → Silver → Gold → Results)")
    
    // Execute pipelines in dependency order
    val dataCleaningResult = executeDataCleaningPipeline(config)
    
    if (dataCleaningResult == PipelineExecutionResult.DataCleaningCompleted) {
      val sessionAnalysisResult = executeSessionAnalysisPipeline(config)
      
      if (sessionAnalysisResult == PipelineExecutionResult.SessionAnalysisCompleted) {
        val rankingResult = executeRankingPipeline(config)
        
        if (rankingResult == PipelineExecutionResult.RankingCompleted) {
          println("✅ Complete pipeline executed successfully")
          PipelineExecutionResult.CompletePipelineCompleted
        } else {
          throw new RuntimeException("Ranking pipeline failed")
        }
      } else {
        throw new RuntimeException("Session analysis pipeline failed")
      }
    } else {
      throw new RuntimeException("Data cleaning pipeline failed")
    }
  }

  /**
   * Displays usage help for command-line interface.
   */
  private def displayUsageHelp(): Unit = {
    println("🎵 Last.fm Session Analysis - Pipeline Orchestration")
    println("=" * 60)
    println("Usage: sbt \"runMain com.lastfm.sessions.Main [pipeline]\"")
    println("")
    println("Available Pipelines:")
    println("  data-cleaning     Execute Bronze → Silver data quality transformation")
    println("  session-analysis  Execute Silver → Gold session calculation")
    println("  ranking           Execute Gold → Results top songs ranking") 
    println("  complete          Execute complete pipeline (default)")
    println("")
    println("Examples:")
    println("  sbt \"runMain com.lastfm.sessions.Main\"                    # Complete pipeline")
    println("  sbt \"runMain com.lastfm.sessions.Main data-cleaning\"      # Data cleaning only")
    println("  sbt \"runMain com.lastfm.sessions.Main session-analysis\"   # Session analysis only")
    println("=" * 60)
  }

  /**
   * Simple resource management utility (Scala 2.13 Using equivalent).
   */
  private def using[T <: AutoCloseable, R](resource: T)(block: T => R): R = {
    try {
      block(resource)
    } finally {
      if (resource != null) {
        try {
          resource.close()
        } catch {
          case NonFatal(_) => // Ignore cleanup failures
        }
      }
    }
  }
}

/**
 * Pipeline execution result enumeration for monitoring and control flow.
 */
sealed trait PipelineExecutionResult

object PipelineExecutionResult {
  case object DataCleaningCompleted extends PipelineExecutionResult
  case object SessionAnalysisCompleted extends PipelineExecutionResult  
  case object RankingCompleted extends PipelineExecutionResult
  case object CompletePipelineCompleted extends PipelineExecutionResult
  case object InvalidArguments extends PipelineExecutionResult
  case object ExecutionFailed extends PipelineExecutionResult
}