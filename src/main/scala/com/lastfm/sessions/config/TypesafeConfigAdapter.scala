package com.lastfm.sessions.config

import com.typesafe.config.{Config, ConfigFactory, ConfigException}
import scala.util.{Try, Success, Failure}

/**
 * Adapter for Typesafe Config, implementing the ConfigurationPort.
 * This class bridges the domain's configuration needs with the Typesafe Config library.
 */
class TypesafeConfigAdapter(config: Config) extends ConfigurationPort {
  
  def this() = this(ConfigFactory.load())
  
  override def getString(path: String): Try[String] = 
    Try(config.getString(path))
  
  override def getInt(path: String): Try[Int] = 
    Try(config.getInt(path))
  
  override def getBoolean(path: String): Try[Boolean] = 
    Try(config.getBoolean(path))
  
  override def getDouble(path: String): Try[Double] = 
    Try(config.getDouble(path))
  
  override def getStringOrDefault(path: String, default: String): String = 
    if (config.hasPath(path)) config.getString(path) else default
  
  override def getIntOrDefault(path: String, default: Int): Int = 
    if (config.hasPath(path)) config.getInt(path) else default
  
  override def getBooleanOrDefault(path: String, default: Boolean): Boolean = 
    if (config.hasPath(path)) config.getBoolean(path) else default
  
  override def getDoubleOrDefault(path: String, default: Double): Double = 
    if (config.hasPath(path)) config.getDouble(path) else default
  
  override def hasPath(path: String): Boolean = 
    config.hasPath(path)
  
  override def getEnvironment(): String = 
    getStringOrDefault("environment", "development")
}

/**
 * Factory for creating configuration adapters based on environment
 */
object TypesafeConfigAdapter {
  
  /**
   * Creates a configuration adapter for the current environment
   * @return The configuration adapter
   */
  def forEnvironment(): TypesafeConfigAdapter = {
    val env = sys.env.getOrElse("ENV", "development")
    val config = env match {
      case "production" => 
        ConfigFactory.load("application-prod.conf")
          .withFallback(ConfigFactory.load())
      case "test" => 
        ConfigFactory.load("application-test.conf")
          .withFallback(ConfigFactory.load())
      case _ => 
        ConfigFactory.load()
    }
    new TypesafeConfigAdapter(config.resolve())
  }
  
  /**
   * Creates a configuration adapter with custom overrides
   * @param overrides Map of configuration overrides
   * @return The configuration adapter
   */
  def withOverrides(overrides: Map[String, Any]): TypesafeConfigAdapter = {
    import scala.jdk.CollectionConverters._
    val overrideConfig = ConfigFactory.parseMap(overrides.asJava)
    val baseConfig = ConfigFactory.load()
    new TypesafeConfigAdapter(overrideConfig.withFallback(baseConfig).resolve())
  }
}