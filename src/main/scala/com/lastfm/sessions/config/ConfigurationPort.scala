package com.lastfm.sessions.config

import scala.util.Try

/**
 * Port for configuration management in hexagonal architecture.
 * This defines the contract for accessing configuration without
 * coupling to specific configuration libraries.
 */
trait ConfigurationPort {
  
  /**
   * Retrieves a string configuration value
   * @param path The configuration path (e.g., "spark.app.name")
   * @return Try containing the configuration value or failure
   */
  def getString(path: String): Try[String]
  
  /**
   * Retrieves an integer configuration value
   * @param path The configuration path
   * @return Try containing the configuration value or failure
   */
  def getInt(path: String): Try[Int]
  
  /**
   * Retrieves a boolean configuration value
   * @param path The configuration path
   * @return Try containing the configuration value or failure
   */
  def getBoolean(path: String): Try[Boolean]
  
  /**
   * Retrieves a double configuration value
   * @param path The configuration path
   * @return Try containing the configuration value or failure
   */
  def getDouble(path: String): Try[Double]
  
  /**
   * Retrieves a string configuration value with a default
   * @param path The configuration path
   * @param default The default value if path doesn't exist
   * @return The configuration value or default
   */
  def getStringOrDefault(path: String, default: String): String
  
  /**
   * Retrieves an integer configuration value with a default
   * @param path The configuration path
   * @param default The default value if path doesn't exist
   * @return The configuration value or default
   */
  def getIntOrDefault(path: String, default: Int): Int
  
  /**
   * Retrieves a boolean configuration value with a default
   * @param path The configuration path
   * @param default The default value if path doesn't exist
   * @return The configuration value or default
   */
  def getBooleanOrDefault(path: String, default: Boolean): Boolean
  
  /**
   * Retrieves a double configuration value with a default
   * @param path The configuration path
   * @param default The default value if path doesn't exist
   * @return The configuration value or default
   */
  def getDoubleOrDefault(path: String, default: Double): Double
  
  /**
   * Checks if a configuration path exists
   * @param path The configuration path
   * @return true if the path exists, false otherwise
   */
  def hasPath(path: String): Boolean
  
  /**
   * Retrieves the current environment (dev, test, prod)
   * @return The current environment
   */
  def getEnvironment(): String
}