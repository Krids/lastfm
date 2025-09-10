import sbt._

object Dependencies {
  
  // Versions
  val sparkVersion = "3.5.0"
  val scalaTestVersion = "3.2.17"
  val scalaCheckVersion = "1.17.0"
  val scalaMockVersion = "5.2.0"
  val typesafeConfigVersion = "1.4.3"
  val logbackVersion = "1.4.11"
  
  // Core dependencies
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion
  val logback = "ch.qos.logback" % "logback-classic" % logbackVersion
  
  // Test dependencies
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
  val scalaCheck = "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test
  val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion % Test
  val scalaTestPlusScalaCheck = "org.scalatestplus" %% "scalacheck-1-17" % "3.2.17.0" % Test
  
  // All dependencies
  val all: Seq[ModuleID] = Seq(
    sparkCore,
    sparkSql,
    typesafeConfig,
    logback,
    scalaTest,
    scalaCheck,
    scalaMock,
    scalaTestPlusScalaCheck
  )
}
