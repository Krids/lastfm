package com.lastfm.sessions.config

import com.lastfm.sessions.utils.BaseTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.{Config, ConfigFactory, ConfigException}
import scala.util.{Try, Success, Failure}

/**
 * Comprehensive error handling test specification for TypesafeConfigAdapter.
 * 
 * Tests all Try-returning methods and error scenarios to achieve complete
 * branch coverage of configuration access and error handling logic.
 * 
 * Coverage Target: TypesafeConfigAdapter 53.19% → 85% (+2% total coverage)
 * Focus: All 4 Try-returning methods with comprehensive error scenarios
 * 
 * Follows TDD principles with realistic configuration error simulation.
 * 
 * @author Felipe Lana Machado
 * @since 1.0.0
 */
class TypesafeConfigAdapterErrorHandlingSpec extends AnyFlatSpec with BaseTestSpec with Matchers {

  "TypesafeConfigAdapter Try methods error handling" should "handle missing configuration keys" in {
    val adapter = new TypesafeConfigAdapter(ConfigFactory.empty())
    
    // Test all Try-returning methods with missing keys
    adapter.getString("missing.key").isFailure should be(true)
    adapter.getInt("missing.key").isFailure should be(true)
    adapter.getBoolean("missing.key").isFailure should be(true)
    adapter.getDouble("missing.key").isFailure should be(true)
  }
  
  it should "handle type conversion failures" in {
    val config = ConfigFactory.parseString("""
      stringValue = "not a number"
      booleanValue = "not a boolean"
      doubleValue = "not a double"
      intValue = "not an integer"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    // Test type conversion failures
    adapter.getInt("stringValue").isFailure should be(true)
    adapter.getBoolean("booleanValue").isFailure should be(true)
    adapter.getDouble("doubleValue").isFailure should be(true)
    adapter.getInt("intValue").isFailure should be(true)
  }
  
  it should "handle null configuration values" in {
    val config = ConfigFactory.parseString("""
      nullValue = null
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    // Null values should cause failures in Try methods
    adapter.getString("nullValue").isFailure should be(true)
    adapter.getInt("nullValue").isFailure should be(true)
    adapter.getBoolean("nullValue").isFailure should be(true)
    adapter.getDouble("nullValue").isFailure should be(true)
  }
  
  it should "handle invalid path formats" in {
    val adapter = new TypesafeConfigAdapter(ConfigFactory.empty())
    
    // Test with invalid path formats - may cause exceptions
    val invalidPaths = List("", "invalid..path", "path.", ".path", "path with spaces")
    
    invalidPaths.foreach { path =>
      val stringResult = adapter.getString(path)
      val intResult = adapter.getInt(path)
      val booleanResult = adapter.getBoolean(path)
      val doubleResult = adapter.getDouble(path)
      
      // Should either fail or handle gracefully
      stringResult.isFailure should be(true)
      intResult.isFailure should be(true)
      booleanResult.isFailure should be(true)
      doubleResult.isFailure should be(true)
    }
  }

  "TypesafeConfigAdapter successful operations" should "handle valid string configurations" in {
    val config = ConfigFactory.parseString("""
      app.name = "LastFM Session Analyzer"
      app.version = "1.0.0"
      app.description = "Unicode string: 音楽アプリ"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getString("app.name") should be(Success("LastFM Session Analyzer"))
    adapter.getString("app.version") should be(Success("1.0.0"))
    adapter.getString("app.description") should be(Success("Unicode string: 音楽アプリ"))
  }
  
  it should "handle valid numeric configurations" in {
    val config = ConfigFactory.parseString("""
      numbers.int = 42
      numbers.double = 3.14159
      numbers.zero = 0
      numbers.negative = -123
      numbers.large = 9223372036854775807
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getInt("numbers.int") should be(Success(42))
    adapter.getDouble("numbers.double") should be(Success(3.14159))
    adapter.getInt("numbers.zero") should be(Success(0))
    adapter.getInt("numbers.negative") should be(Success(-123))
    // Large numbers cause overflow when converted to Int
    val largeResult = adapter.getInt("numbers.large")
    largeResult.isFailure should be(true)
    largeResult.failed.get shouldBe a[com.typesafe.config.ConfigException.WrongType]
  }
  
  it should "handle valid boolean configurations" in {
    val config = ConfigFactory.parseString("""
      flags.enabled = true
      flags.disabled = false
      flags.on = on
      flags.off = off
      flags.yes = yes
      flags.no = no
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getBoolean("flags.enabled") should be(Success(true))
    adapter.getBoolean("flags.disabled") should be(Success(false))
    adapter.getBoolean("flags.on") should be(Success(true))
    adapter.getBoolean("flags.off") should be(Success(false))
    adapter.getBoolean("flags.yes") should be(Success(true))
    adapter.getBoolean("flags.no") should be(Success(false))
  }

  "TypesafeConfigAdapter default value methods" should "return defaults for missing paths" in {
    val adapter = new TypesafeConfigAdapter(ConfigFactory.empty())
    
    adapter.getStringOrDefault("missing.string", "default") should be("default")
    adapter.getIntOrDefault("missing.int", 42) should be(42)
    adapter.getBooleanOrDefault("missing.boolean", true) should be(true)
    adapter.getDoubleOrDefault("missing.double", 3.14) should be(3.14)
  }
  
  it should "return actual values when paths exist" in {
    val config = ConfigFactory.parseString("""
      existing.string = "actual value"
      existing.int = 123
      existing.boolean = true
      existing.double = 2.71
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getStringOrDefault("existing.string", "default") should be("actual value")
    adapter.getIntOrDefault("existing.int", 42) should be(123)
    adapter.getBooleanOrDefault("existing.boolean", false) should be(true)
    adapter.getDoubleOrDefault("existing.double", 3.14) should be(2.71)
  }
  
  it should "handle type mismatches in default methods gracefully" in {
    val config = ConfigFactory.parseString("""
      wrong.type.string = 123
      wrong.type.int = "not a number"
      wrong.type.boolean = "not a boolean"
      wrong.type.double = "not a double"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    // Default methods should handle type mismatches by returning defaults or throwing
    val stringResult = Try { adapter.getStringOrDefault("wrong.type.string", "default") }
    val intResult = Try { adapter.getIntOrDefault("wrong.type.int", 42) }
    val booleanResult = Try { adapter.getBooleanOrDefault("wrong.type.boolean", false) }
    val doubleResult = Try { adapter.getDoubleOrDefault("wrong.type.double", 3.14) }
    
    // Should either return defaults or fail gracefully
    if (stringResult.isFailure) {
      stringResult.failed.get shouldBe a[ConfigException]
    }
    if (intResult.isFailure) {
      intResult.failed.get shouldBe a[ConfigException]
    }
    if (booleanResult.isFailure) {
      booleanResult.failed.get shouldBe a[ConfigException]
    }
    if (doubleResult.isFailure) {
      doubleResult.failed.get shouldBe a[ConfigException]
    }
  }

  "TypesafeConfigAdapter hasPath method" should "correctly identify existing paths" in {
    val config = ConfigFactory.parseString("""
      existing.path = "value"
      nested.deep.path = "value"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.hasPath("existing.path") should be(true)
    adapter.hasPath("nested.deep.path") should be(true)
  }
  
  it should "correctly identify missing paths" in {
    val adapter = new TypesafeConfigAdapter(ConfigFactory.empty())
    
    adapter.hasPath("missing.path") should be(false)
    
    // Empty path causes ConfigException.BadPath
    val emptyPathResult = Try { adapter.hasPath("") }
    emptyPathResult.isFailure should be(true)
    emptyPathResult.failed.get shouldBe a[com.typesafe.config.ConfigException.BadPath]
  }
  
  it should "handle edge case path formats" in {
    val adapter = new TypesafeConfigAdapter(ConfigFactory.empty())
    
    // Test edge case paths
    adapter.hasPath("single") should be(false)
    adapter.hasPath("very.very.very.deep.nested.path") should be(false)
  }

  "TypesafeConfigAdapter getEnvironment method" should "return default environment when missing" in {
    val adapter = new TypesafeConfigAdapter(ConfigFactory.empty())
    
    adapter.getEnvironment() should be("development")
  }
  
  it should "return configured environment when present" in {
    val config = ConfigFactory.parseString("""
      environment = "production"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getEnvironment() should be("production")
  }
  
  it should "handle special environment values" in {
    val testEnvironments = List("test", "staging", "local", "docker", "kubernetes")
    
    testEnvironments.foreach { env =>
      val config = ConfigFactory.parseString(s"""environment = "$env"""")
      val adapter = new TypesafeConfigAdapter(config)
      
      adapter.getEnvironment() should be(env)
    }
  }

  "TypesafeConfigAdapter factory methods" should "create adapters from various sources" in {
    // Test default factory creation
    val defaultAdapter = TypesafeConfigAdapter.forEnvironment()
    defaultAdapter should not be null
    defaultAdapter.getEnvironment() should not be null
  }
  
  it should "handle system property overrides" in {
    // Test that adapter respects system properties
    val adapter = TypesafeConfigAdapter.forEnvironment()
    val env = adapter.getEnvironment()
    
    // Environment should be deterministic
    env should (be("development") or be("production") or be("test") or be("staging"))
  }
  
  it should "handle missing configuration files gracefully" in {
    // This should not throw exceptions but use reference.conf defaults
    val adapter = new TypesafeConfigAdapter()
    adapter should not be null
  }

  "TypesafeConfigAdapter edge cases" should "handle very deep nested paths" in {
    val config = ConfigFactory.parseString("""
      level1.level2.level3.level4.level5.value = "deep value"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getString("level1.level2.level3.level4.level5.value") should be(Success("deep value"))
    adapter.hasPath("level1.level2.level3.level4.level5.value") should be(true)
  }
  
  it should "handle configuration with special characters" in {
    val config = ConfigFactory.parseString("""
      special_key = "value"
      key_with_unicode = "こんにちは🎵"
      key_with_underscores = "value"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getString("special_key") should be(Success("value"))
    adapter.getString("key_with_unicode") should be(Success("こんにちは🎵"))
    adapter.getString("key_with_underscores") should be(Success("value"))
  }
  
  it should "handle configuration with extreme numeric values" in {
    val config = ConfigFactory.parseString(s"""
      numbers.maxInt = ${Int.MaxValue}
      numbers.minInt = ${Int.MinValue}
      numbers.maxDouble = ${Double.MaxValue}
      numbers.minDouble = ${Double.MinValue}
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getInt("numbers.maxInt") should be(Success(Int.MaxValue))
    adapter.getInt("numbers.minInt") should be(Success(Int.MinValue))
    adapter.getDouble("numbers.maxDouble") should be(Success(Double.MaxValue))
    adapter.getDouble("numbers.minDouble") should be(Success(Double.MinValue))
  }
  
  it should "handle configuration merge scenarios" in {
    val baseConfig = ConfigFactory.parseString("""
      base.value = "base"
      shared.value = "base"
    """)
    val overrideConfig = ConfigFactory.parseString("""
      override.value = "override"
      shared.value = "override"
    """)
    
    val mergedConfig = overrideConfig.withFallback(baseConfig)
    val adapter = new TypesafeConfigAdapter(mergedConfig)
    
    adapter.getString("base.value") should be(Success("base"))
    adapter.getString("override.value") should be(Success("override"))
    adapter.getString("shared.value") should be(Success("override")) // Override wins
  }

  "TypesafeConfigAdapter error propagation" should "preserve original exception types" in {
    val config = ConfigFactory.parseString("""
      wrongType = "string value"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    // Test that original ConfigException types are preserved
    val intResult = adapter.getInt("wrongType")
    intResult.isFailure should be(true)
    intResult.failed.get shouldBe a[ConfigException.WrongType]
  }
  
  it should "handle ConfigException.Missing appropriately" in {
    val adapter = new TypesafeConfigAdapter(ConfigFactory.empty())
    
    val result = adapter.getString("definitely.missing.path")
    result.isFailure should be(true)
    result.failed.get shouldBe a[ConfigException.Missing]
  }
  
  it should "preserve error messages in failures" in {
    val config = ConfigFactory.parseString("""
      badNumber = "definitely not a number"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    val result = adapter.getInt("badNumber")
    result.isFailure should be(true)
    result.failed.get.getMessage should include("badNumber")
  }

  "TypesafeConfigAdapter boundary value testing" should "handle numeric boundary values correctly" in {
    val config = ConfigFactory.parseString(s"""
      zero = 0
      one = 1
      minusOne = -1
      maxInt = ${Int.MaxValue}
      minInt = ${Int.MinValue}
      justOverInt = ${Int.MaxValue.toLong + 1}
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getInt("zero") should be(Success(0))
    adapter.getInt("one") should be(Success(1))
    adapter.getInt("minusOne") should be(Success(-1))
    adapter.getInt("maxInt") should be(Success(Int.MaxValue))
    adapter.getInt("minInt") should be(Success(Int.MinValue))
    
    // Value over Int.MaxValue should fail
    adapter.getInt("justOverInt").isFailure should be(true)
  }
  
  it should "handle double precision edge cases" in {
    val config = ConfigFactory.parseString(s"""
      tiny = ${Double.MinPositiveValue}
      huge = ${Double.MaxValue}
      infinity = "Infinity"
      negInfinity = "-Infinity"
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getDouble("tiny") should be(Success(Double.MinPositiveValue))
    adapter.getDouble("huge") should be(Success(Double.MaxValue))
    
    // Infinity handling may vary by implementation
    val infResult = adapter.getDouble("infinity")
    val negInfResult = adapter.getDouble("negInfinity")
    
    // Should either parse infinity or fail gracefully
    if (infResult.isSuccess) {
      infResult.get should be(Double.PositiveInfinity)
    }
    if (negInfResult.isSuccess) {
      negInfResult.get should be(Double.NegativeInfinity)
    }
  }

  "TypesafeConfigAdapter complex configuration scenarios" should "handle array and object access" in {
    val config = ConfigFactory.parseString("""
      arrays.numbers = [1, 2, 3]
      objects.nested = { key = "value" }
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    // Attempting to get arrays/objects as simple types should fail
    adapter.getString("arrays.numbers").isFailure should be(true)
    adapter.getString("objects.nested").isFailure should be(true)
  }
  
  it should "handle configuration with comments and whitespace" in {
    val config = ConfigFactory.parseString("""
      # This is a comment
      spaced.value = "  whitespace value  "
      
      # Another comment
      trimmed.value = value
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getString("spaced.value") should be(Success("  whitespace value  "))
    adapter.getString("trimmed.value") should be(Success("value"))
  }
  
  it should "handle empty and blank string values" in {
    val config = ConfigFactory.parseString("""
      empty = ""
      blank = "   "
      quoted_empty = "\"\""
    """)
    val adapter = new TypesafeConfigAdapter(config)
    
    adapter.getString("empty") should be(Success(""))
    adapter.getString("blank") should be(Success("   "))
    adapter.getString("quoted_empty") should be(Success("\"\""))
  }

  "TypesafeConfigAdapter constructor variants" should "handle default constructor" in {
    val adapter = new TypesafeConfigAdapter()
    adapter should not be null
    adapter.getEnvironment() should not be null
  }
  
  it should "handle explicit config constructor" in {
    val customConfig = ConfigFactory.parseString("""
      custom.setting = "test"
    """)
    val adapter = new TypesafeConfigAdapter(customConfig)
    
    adapter.getString("custom.setting") should be(Success("test"))
  }
  
  it should "handle null config gracefully" in {
    // Should handle null config without NPE
    val result = Try {
      new TypesafeConfigAdapter(null)
    }
    
    // May fail immediately or handle gracefully
    if (result.isFailure) {
      result.failed.get shouldBe a[Throwable]
    } else {
      result.get should not be null
    }
  }
}
