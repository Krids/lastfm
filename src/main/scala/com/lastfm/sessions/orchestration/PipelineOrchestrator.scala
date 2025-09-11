package com.lastfm.sessions.orchestration

import com.lastfm.sessions.pipelines.{PipelineConfig, DataCleaningPipeline}
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
   * Executes Data Cleaning Pipeline (Bronze → Silver transformation).
   */
  private def executeDataCleaningPipeline(config: PipelineConfig): PipelineExecutionResult = {
    println("🧹 Executing Data Cleaning Pipeline (Bronze → Silver)")
    
    using(SparkSessionManager.createProductionSession()) { implicit spark =>
      val pipeline = new DataCleaningPipeline(config)
      val result = pipeline.execute()
      
      result match {
        case Success(qualityMetrics: DataQualityMetrics) =>
          println(s"✅ Data cleaning completed successfully")
          println(f"   Quality Score: ${qualityMetrics.qualityScore}%.6f%%")
          println(s"   Records: ${qualityMetrics.totalRecords} → ${qualityMetrics.validRecords}")
          PipelineExecutionResult.DataCleaningCompleted
          
        case Failure(exception) =>
          throw new RuntimeException("Data cleaning pipeline failed", exception)
      }
    }
  }

  /**
   * Executes Session Analysis Pipeline (Silver → Gold transformation).
   * Future implementation - currently returns placeholder.
   */
  private def executeSessionAnalysisPipeline(config: PipelineConfig): PipelineExecutionResult = {
    println("🔄 Session Analysis Pipeline - Not yet implemented")
    println("   Next: UserSession domain model → SessionCalculator → Session analysis")
    PipelineExecutionResult.SessionAnalysisCompleted
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