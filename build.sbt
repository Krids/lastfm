ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.13.16"  // Updated to latest stable 2.13.x
ThisBuild / organization := "com.lastfm"

// Java version configuration
ThisBuild / javacOptions ++= Seq("-source", "11", "-target", "11")
ThisBuild / javaOptions ++= Seq("-Xmx2g", "-XX:+UseG1GC")

lazy val root = (project in file("."))
  .settings(
    name := "lastfm-session-analyzer",
    
    // Dependencies with SLF4J conflict resolution
    libraryDependencies ++= Dependencies.all.map {
      case dep if dep.organization == "org.apache.spark" => 
        dep exclude("org.slf4j", "slf4j-log4j12")
      case dep => dep
    },
    
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
    
    // Test configuration with safety validation
    Test / testOptions ++= Seq(
      Tests.Argument("-oDF"),
      Tests.Setup { () =>
        println("🔍 Validating test environment safety before test execution...")
        
        // Ensure test directories exist
        val testDirs = Seq("data/test", "data/test/bronze", "data/test/silver", "data/test/gold", "data/test/results")
        testDirs.foreach { dir =>
          val path = java.nio.file.Paths.get(dir)
          if (!java.nio.file.Files.exists(path)) {
            java.nio.file.Files.createDirectories(path)
            println(s"📁 Created test directory: $dir")
          }
        }
        
        // Critical safety check: Validate no production contamination before tests
        val productionDirs = Seq("data/output/bronze", "data/output/silver", "data/output/gold", "data/output/results")
        val contaminatedFiles = productionDirs.flatMap { dir =>
          val dirPath = java.nio.file.Paths.get(dir)
          if (java.nio.file.Files.exists(dirPath) && java.nio.file.Files.isDirectory(dirPath)) {
            scala.util.Try {
              java.nio.file.Files.list(dirPath)
                .filter { p =>
                  val fileName = p.getFileName.toString.toLowerCase
                  fileName.contains("test") || fileName.startsWith("temp-") || fileName.startsWith("tmp-") ||
                  fileName.contains("spec") || fileName.contains("fixture")
                }
                .toArray
                .map(_.toString)
            }.getOrElse(Array.empty[String])
          } else {
            Array.empty[String]
          }
        }
        
        if (contaminatedFiles.nonEmpty) {
          val contaminationDetails = contaminatedFiles.mkString("\n  - ")
          throw new RuntimeException(
            s"🚨 CRITICAL: Production contamination detected before test execution!\n" +
            s"The following test artifacts were found in production directories:\n  - $contaminationDetails\n\n" +
            s"This indicates previous test safety violations. Clean these files before running tests:\n" +
            s"  rm -rf ${contaminatedFiles.mkString(" ")}\n\n" +
            s"Future tests will be isolated to data/test/ to prevent this issue."
          )
        }
        
        println("✅ Test environment safety validated - no production contamination detected")
        println("🧪 Tests will execute in isolated environment: data/test/")
      },
      Tests.Cleanup { () =>
        println("🧹 Post-test safety validation and cleanup...")
        
        // Verify no production contamination occurred during test execution
        val productionDirs = Seq("data/output/bronze", "data/output/silver", "data/output/gold", "data/output/results")
        val newContamination = productionDirs.flatMap { dir =>
          val dirPath = java.nio.file.Paths.get(dir)
          if (java.nio.file.Files.exists(dirPath) && java.nio.file.Files.isDirectory(dirPath)) {
            scala.util.Try {
              java.nio.file.Files.list(dirPath)
                .filter { p =>
                  val fileName = p.getFileName.toString.toLowerCase
                  fileName.contains("test") || fileName.startsWith("temp-") || fileName.startsWith("tmp-")
                }
                .toArray
                .map(_.toString)
            }.getOrElse(Array.empty[String])
          } else {
            Array.empty[String]
          }
        }
        
        if (newContamination.nonEmpty) {
          val contaminationDetails = newContamination.mkString("\n  - ")
          System.err.println(
            s"🚨 CRITICAL: Test execution contaminated production directories!\n" +
            s"Contaminated files:\n  - $contaminationDetails\n\n" +
            s"This indicates a test safety violation. Immediate cleanup required."
          )
          
          // Attempt automatic cleanup (with caution)
          newContamination.foreach { filePath =>
            scala.util.Try {
              java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(filePath))
              println(s"🧹 Cleaned contaminated file: $filePath")
            }.recover {
              case ex => System.err.println(s"⚠️ Could not clean $filePath: ${ex.getMessage}")
            }
          }
        } else {
          println("✅ Test cleanup validated - no production contamination detected")
        }
      }
    ),
    Test / logBuffered := false,
    Test / parallelExecution := false,
    
    // Fork JVM for both main execution and tests (CRITICAL for applying JVM options)
    fork := true,
    Test / fork := true,
    
    // Code coverage configuration
    coverageEnabled := true,
    coverageMinimumStmtTotal := 80,
    coverageMinimumBranchTotal := 70,
    coverageFailOnMinimum := false, // Set to true once coverage targets are met
    
    // Coverage exclusions for generated/infrastructure code
    coverageExcludedPackages := Seq(
      "<empty>",  // Empty package
      ".*\\.infrastructure\\..*", // Infrastructure adapters  
      ".*\\.config\\..*",  // Configuration classes
      ".*Main.*"  // Main application entry points
    ).mkString(";"),
    
    // Coverage data directory
    coverageDataDir := target.value / "scala-2.13" / "scoverage-data",
    
    // Main execution JVM options (Java 11 compatible)
    run / javaOptions ++= Seq(
      // Memory management
      "-Xmx12g",                        // 12GB heap for development
      "-XX:+UseG1GC",                   // G1 garbage collector
      "-XX:G1HeapRegionSize=16m",       // Optimal region size
      "-XX:MaxGCPauseMillis=200",       // Low-latency GC
      
      // Spark configuration
      "-Dspark.master=local[16]",       // Use optimal partition count
      "-Dspark.app.name=LastFMSessionAnalyzer-Production",
      "-Dspark.sql.shuffle.partitions=16",
      "-Dspark.sql.adaptive.enabled=true",
      "-Dspark.sql.adaptive.advisoryPartitionSizeInBytes=128m",
      
      // Hadoop compatibility
      "-Dhadoop.home.dir=/tmp",
      
      // Java 11 compatibility exports
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-exports=java.base/sun.nio.cs=ALL-UNNAMED", 
      "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
      "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
      
      // Java 11 compatibility opens
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      
      // Performance and debugging (Java 11 compatible)
      "-XX:+UnlockExperimentalVMOptions",
      "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
    ),
    Test / javaOptions ++= Seq(
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