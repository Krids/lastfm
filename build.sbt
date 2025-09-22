ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.13.12"
ThisBuild / organization := "com.lastfm"

// Java 11 configuration for Spark compatibility
ThisBuild / javacOptions ++= Seq("-source", "11", "-target", "11")

lazy val root = (project in file("."))
  .settings(
    name := "lastfm-session-analyzer",
    
    // Essential dependencies for data engineering
    libraryDependencies ++= Seq(
      // Core Spark dependencies
      "org.apache.spark" %% "spark-core" % "3.5.0" exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.spark" %% "spark-sql" % "3.5.0" exclude("org.slf4j", "slf4j-log4j12"),
      
      // Configuration management
      "com.typesafe" % "config" % "1.4.3",
      
      // Logging (resolve SLF4J conflicts)
      "ch.qos.logback" % "logback-classic" % "1.4.11",
      
      // Testing dependencies
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalacheck" %% "scalacheck" % "1.17.0" % Test
    ),
    
    // Resolve SLF4J version conflicts
    dependencyOverrides ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.36"
    ),
    
    // Clean Scala compiler options
    scalacOptions ++= Seq(
      "-feature",
      "-deprecation", 
      "-unchecked"
    ),
    
    // Simple test configuration
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-oDF"),
    
    // Development JVM options (optimized for local development)
    run / fork := true,
    run / javaOptions ++= Seq(
      // Memory configuration for data processing
      "-Xmx8g",
      "-XX:+UseG1GC",
      
      // Spark configuration for local development
      "-Dspark.master=local[*]",
      "-Dspark.app.name=LastFM-Session-Analysis",
      "-Dspark.sql.adaptive.enabled=true",
      "-Dspark.sql.adaptive.coalescePartitions.enabled=true",
      
      // Java 11 compatibility (essential for Spark 3.5)
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    
    // Test JVM options (lighter configuration)
    Test / javaOptions ++= Seq(
      "-Xmx4g",
      "-Dspark.master=local[2]",
      "-Dspark.sql.shuffle.partitions=2",
      
      // Java 11 compatibility for tests
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
  )
