package com.lastfm

import com.lastfm.spark.SparkManager
import com.lastfm.config.Config
import com.lastfm.pipelines.{DataCleaningPipeline, DistributedSessionPipeline, RankingPipeline}
import scala.util.{Success, Failure}
import java.time.LocalDateTime

/**
 * LastFM Session Analysis - Portfolio Data Engineering Project
 * 
 * A clean, production-ready data engineering solution demonstrating:
 * - Advanced Spark optimization with strategic partitioning
 * - Distributed processing for scalable data analysis  
 * - Three-pipeline architecture (Bronze → Silver → Gold → Results)
 * - Business logic implementation (20-minute session gap algorithm)
 * - Performance engineering best practices
 * 
 * Technical Highlights:
 * - Strategic userId partitioning eliminates shuffle operations
 * - Distributed window functions for memory-efficient session analysis
 * - Medallion architecture with proper data layer separation
 * - Advanced coalesce vs repartition optimization
 * - Clean architecture with separated concerns
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
object Main extends App {
  
  private val startTime = System.currentTimeMillis()
  
  println("🎵 LastFM Session Analysis - Data Engineering Pipeline")
  println("=" * 80)
  println(s"🕐 Started: ${LocalDateTime.now()}")
  println(s"☕ Java Version: ${System.getProperty("java.version")}")
  println(s"🖥️  Available Cores: ${Runtime.getRuntime.availableProcessors()}")
  println(s"💾 Available Memory: ${Runtime.getRuntime.maxMemory() / 1024 / 1024 / 1024}GB")
  
  val pipelineType = args.headOption.getOrElse("complete")
  println(s"🎯 Selected Pipeline: '$pipelineType'")
  
  // Load configuration and create Spark session
  val config = Config.load()
  val spark = SparkManager.createSession()
  
  try {
    val result = pipelineType match {
      case "data-cleaning" => runDataCleaning()
      case "session-analysis" => runSessionAnalysis() 
      case "ranking" => runRanking()
      case "help" | "--help" => 
        displayUsageHelp()
        Success(())
      case "complete" | _ => runCompletePipeline()
    }
    
    result match {
      case Success(_) => 
        displaySuccessSummary()
        println("✅ Pipeline completed successfully!")
      case Failure(exception) => 
        displayFailureSummary(exception)
        sys.exit(1)
    }
    
  } catch {
    case exception: Exception =>
      displayFailureSummary(exception)
      sys.exit(1)
  } finally {
    spark.stop()
  }
  
  /**
   * Executes the complete three-pipeline workflow.
   * Demonstrates proper pipeline orchestration and error handling.
   */
  private def runCompletePipeline() = {
    println("\n🔄 Complete Pipeline: Bronze → Silver → Gold → Results")
    
    for {
      _ <- runDataCleaning()
      _ <- runSessionAnalysis()
      _ <- runRanking()
    } yield {
      println("\n🎉 Complete workflow executed successfully!")
      println("   📊 Data Quality → ✅ Complete")
      println("   📈 Session Analysis → ✅ Complete")
      println("   🏆 Song Ranking → ✅ Complete")
    }
  }
  
  /**
   * Bronze → Silver: Data cleaning with strategic partitioning.
   */
  private def runDataCleaning() = {
    println("\n🧹 Data Cleaning Pipeline: Bronze → Silver")
    val pipeline = new DataCleaningPipeline(spark, config)
    pipeline.execute()
  }
  
  /**
   * Silver → Gold: Distributed session analysis.
   */
  private def runSessionAnalysis() = {
    println("\n📈 Session Analysis Pipeline: Silver → Gold")  
    val pipeline = new DistributedSessionPipeline(spark, config)
    pipeline.execute()
  }
  
  /**
   * Gold → Results: Song ranking and final output.
   */
  private def runRanking() = {
    println("\n🎶 Ranking Pipeline: Gold → Results")
    val pipeline = new RankingPipeline(spark, config)
    pipeline.execute()
  }
  
  /**
   * Displays usage help and available pipeline options.
   */
  private def displayUsageHelp(): Unit = {
    println("\n📋 Usage: sbt \"runMain com.lastfm.Main [pipeline]\"")
    println("\n🔄 Available Pipelines:")
    println("   complete         Run all pipelines (Bronze → Silver → Gold → Results)")
    println("   data-cleaning    Bronze → Silver: Clean and partition data")
    println("   session-analysis Silver → Gold: Analyze user sessions") 
    println("   ranking          Gold → Results: Generate top songs ranking")
    println("   help             Show this help message")
    
    println("\n🐳 Docker Usage:")
    println("   docker-compose up                    # Complete pipeline")
    println("   docker run lastfm data-cleaning     # Individual pipeline")
    
    println("\n📊 Expected Output:")
    println("   data/output/results/top_songs.tsv   # Final top 10 songs")
    
    println("\n🔧 Technical Highlights:")
    println("   • Strategic userId partitioning (zero-shuffle operations)")
    println("   • Distributed window functions (unlimited scalability)")
    println("   • Medallion architecture (Bronze/Silver/Gold layers)")
    println("   • Advanced Spark optimization (coalesce, caching, compression)")
  }
  
  /**
   * Displays success summary with performance metrics.
   */
  private def displaySuccessSummary(): Unit = {
    val totalExecutionTime = System.currentTimeMillis() - startTime
    val executionMinutes = totalExecutionTime / 60000.0
    
    println(s"\n📊 Execution Summary:")
    println(f"   ⏱️  Total Duration: ${executionMinutes}%.2f minutes")
    println(s"   🎯 Pipeline: ${pipelineType}")
    println(s"   🖥️  Processing: Distributed across ${Runtime.getRuntime.availableProcessors()} cores")
    
    // Display next steps based on pipeline executed
    pipelineType match {
      case "data-cleaning" =>
        println(s"\n🚀 Next Steps:")
        println(s"   📄 Silver layer ready at: ${config.outputPath}/silver/")
        println(s"   🎯 Run: sbt \"runMain com.lastfm.Main session-analysis\"")
        
      case "session-analysis" =>
        println(s"\n🚀 Next Steps:")
        println(s"   📄 Gold layer ready at: ${config.outputPath}/gold/")
        println(s"   🎯 Run: sbt \"runMain com.lastfm.Main ranking\"")
        
      case "ranking" =>
        println(s"\n🎯 Final Results:")
        println(s"   📄 Check: ${config.outputPath}/results/top_songs.tsv")
        
      case "complete" | _ =>
        println(s"\n🎉 Complete Analysis Ready!")
        println(s"   📊 Data Processing: ✅ Optimized")
        println(s"   📈 Session Analysis: ✅ Complete") 
        println(s"   🏆 Song Ranking: ✅ Complete")
        println(s"   📄 Final Output: ${config.outputPath}/results/top_songs.tsv")
    }
  }
  
  /**
   * Displays failure summary with troubleshooting guidance.
   */
  private def displayFailureSummary(exception: Throwable): Unit = {
    val executionTime = System.currentTimeMillis() - startTime
    
    println(s"\n❌ Pipeline Execution Failed")
    println("-" * 60)
    println(s"🕐 Failed after: ${executionTime / 1000.0} seconds")
    println(s"🔍 Error: ${exception.getClass.getSimpleName}")
    println(s"📝 Message: ${exception.getMessage}")
    
    println(s"\n🛠️  Troubleshooting:")
    exception match {
      case _: RuntimeException if exception.getMessage != null && exception.getMessage.contains("not found") =>
        println(s"   💡 Verify dataset is available at: ${config.inputPath}")
        println(s"   💡 Check file permissions and directory structure")
        
      case _: OutOfMemoryError =>
        println(s"   💡 Increase memory: sbt -J-Xmx16g \"runMain com.lastfm.Main\"")
        println(s"   💡 Or use Docker for memory management")
        
      case _ =>
        println(s"   💡 Check input data format and file paths")
        println(s"   💡 Verify Spark configuration and available resources")
    }
    
    println(s"\n📋 For help: sbt \"runMain com.lastfm.Main help\"")
  }
}