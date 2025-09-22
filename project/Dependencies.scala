import sbt._

// Simplified dependencies for portfolio-focused project
// Removed excessive dependency abstractions while keeping essential functionality
object Dependencies {
  
  // Core version definitions
  val sparkVersion = "3.5.0"
  val scalaTestVersion = "3.2.17"
  val scalaCheckVersion = "1.17.0"
  val typesafeConfigVersion = "1.4.3"
  val logbackVersion = "1.4.11"
  
  // Essential dependencies only
  val all: Seq[ModuleID] = Seq(
    // Spark for distributed data processing
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    
    // Configuration management
    "com.typesafe" % "config" % typesafeConfigVersion,
    
    // Logging
    "ch.qos.logback" % "logback-classic" % logbackVersion,
    
    // Essential testing
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test
  )
}
