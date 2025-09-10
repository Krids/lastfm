ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.13.14"  // Updated to latest stable 2.13.x
ThisBuild / organization := "com.lastfm"

lazy val root = (project in file("."))
  .settings(
    name := "lastfm-session-analyzer",
    
    // Dependencies with SLF4J conflict resolution
    libraryDependencies ++= Seq(
      // Spark dependencies - exclude conflicting logging
      "org.apache.spark" %% "spark-core" % "3.5.0" exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.spark" %% "spark-sql" % "3.5.0" exclude("org.slf4j", "slf4j-log4j12"),
      
      // Configuration
      "com.typesafe" % "config" % "1.4.3",
      
      // Logging - single provider only
      "ch.qos.logback" % "logback-classic" % "1.4.11",
      
      // Test dependencies
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalacheck" %% "scalacheck" % "1.17.0" % Test,
      "org.scalamock" %% "scalamock" % "5.2.0" % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % "3.2.17.0" % Test
    ),
    
    // Force single SLF4J implementation
    dependencyOverrides ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.36",
      "ch.qos.logback" % "logback-classic" % "1.4.11"
    ),
    
    // Scala compiler options
    scalacOptions ++= Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-language:implicitConversions",
      "-language:postfixOps"
    ),
    
    // Test configuration
    Test / testOptions += Tests.Argument("-oDF"),
    Test / logBuffered := false,
    Test / parallelExecution := false,
    
    // Fork JVM for tests with Java 11 compatibility
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Dspark.master=local[2]",
      "-Dspark.app.name=LastFMSessionAnalyzer-Tests",
      "-Dspark.sql.shuffle.partitions=2",
      "-Xmx2g",
      
      // Hadoop compatibility
      "-Dhadoop.home.dir=/tmp",
      
      // Export internal Java packages to Spark (CRITICAL for Java 11+)
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-exports=java.base/sun.nio.cs=ALL-UNNAMED", 
      "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
      "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
      
      // Open internal Java packages to Spark
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      
      // Suppress warnings
      "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
    )
  )