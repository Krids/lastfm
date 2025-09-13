# Data Cleaning Strategy for Last.fm Session Analysis

## 🎯 **Pipeline Overview**

Based on your analysis results and **ADR-005 Hybrid Data Cleaning Approach**, here's a comprehensive data cleaning plan focused on session analysis requirements.

---

## 🔧 **Data Cleaning Pipeline Steps**

### **Stage 1: Critical Field Validation (FAIL-FAST)**
**Objective**: Reject records that make session analysis impossible

#### **1.1 User ID Validation**
- ✅ **Must Have**: Non-null, non-empty, non-blank validation
- ✅ **Must Have**: Length validation (reasonable bounds: 3-50 characters)
- ✅ **Must Have**: Character set validation (alphanumeric + allowed special chars)
- ✅ **Must Have**: Consistent format validation (all follow same pattern)

#### **1.2 Timestamp Validation**  
- ✅ **Must Have**: Parse ISO 8601 format with timezone handling
- ✅ **Must Have**: Validate timestamp is within reasonable bounds (e.g., 2000-2030)
- ✅ **Must Have**: Detect and reject unparseable timestamps
- ✅ **Must Have**: Convert all to UTC for consistent session calculation
- ✅ **Must Have**: Validate chronological ordering per user (detect time travel)

### **Stage 2: High-Priority Cleansing (CLEANSE & CONTINUE)**
**Objective**: Standardize track identification for accurate song counting

#### **2.1 Artist Name Standardization**
- ✅ **Must Have**: Trim whitespace and normalize spacing
- ✅ **Must Have**: Remove control characters (tabs, newlines, carriage returns)
- ✅ **Must Have**: Handle null/empty values → default to "Unknown Artist"
- ✅ **Must Have**: Length limits (reasonable maximum: 500 chars)
- ✅ **Must Have**: Unicode normalization for international characters
- ✅ **Must Have**: Case-insensitive deduplication hints

#### **2.2 Track Name Standardization**
- ✅ **Must Have**: Identical processing as artist names
- ✅ **Must Have**: Handle null/empty values → default to "Unknown Track"
- ✅ **Must Have**: Remove common noise patterns (feat., featuring, remix indicators)
- ✅ **Must Have**: Standardize punctuation variations

#### **2.3 Track Identity Resolution**
- ✅ **Must Have**: Create composite track key hierarchy:
  1. Use Track MBID if available (88.7% coverage)
  2. Use Artist MBID + track name if track MBID missing (8.2% coverage)  
  3. Use standardized "artist_name — track_name" for remainder (3.1% coverage)
- ✅ **Must Have**: Validate MBID format if present (UUID pattern)
- ✅ **Must Have**: Handle case where both MBID and names are problematic

### **Stage 3: Quality Enhancement (ACCEPT & IMPROVE)**
**Objective**: Improve data quality without blocking processing

#### **3.1 Duplicate Detection**
- ✅ **Must Have**: Identify exact duplicates (same user, timestamp, track)
- ✅ **Must Have**: Identify near-duplicates (same user, track, within 5 seconds)
- ✅ **Must Have**: Configurable deduplication strategy (keep first, keep last, merge)

#### **3.2 Data Enrichment**
- ✅ **Must Have**: Generate session-relevant metadata (play sequence numbers)
- ✅ **Must Have**: Calculate time gaps between consecutive plays per user
- ✅ **Must Have**: Flag potential data quality issues for monitoring

---

## 🧪 **Comprehensive Test Strategy**

### **Unit Tests: Critical Field Validation**

#### **User ID Validation Tests**
```scala
class UserIdValidationTests {
  // Happy Path
  ✅ "should accept valid alphanumeric user IDs"
  ✅ "should accept user IDs with allowed special characters"
  
  // Boundary Cases
  ✅ "should reject null user ID"
  ✅ "should reject empty string user ID" 
  ✅ "should reject whitespace-only user ID"
  ✅ "should reject user ID exceeding maximum length"
  ✅ "should reject user ID below minimum length"
  
  // Edge Cases
  ✅ "should handle user IDs with leading/trailing whitespace"
  ✅ "should reject user IDs with control characters"
  ✅ "should reject user IDs with SQL injection patterns"
  ✅ "should handle Unicode characters in user IDs"
  ✅ "should validate consistent user ID format across dataset"
}
```

#### **Timestamp Validation Tests**
```scala
class TimestampValidationTests {
  // Happy Path
  ✅ "should parse ISO 8601 timestamps with Z suffix"
  ✅ "should parse ISO 8601 timestamps with timezone offsets"
  ✅ "should convert all timestamps to UTC consistently"
  
  // Format Variations
  ✅ "should handle timestamps with milliseconds"
  ✅ "should handle timestamps without milliseconds" 
  ✅ "should reject non-ISO format timestamps"
  ✅ "should handle different timezone formats (+00:00, Z, GMT)"
  
  // Boundary Cases
  ✅ "should reject null timestamps"
  ✅ "should reject empty string timestamps"
  ✅ "should reject unparseable timestamp strings"
  ✅ "should reject timestamps before reasonable start date (2000)"
  ✅ "should reject timestamps after reasonable end date (2030)"
  
  // Edge Cases - Time Logic
  ✅ "should handle leap seconds correctly"
  ✅ "should handle daylight saving time transitions"
  ✅ "should handle midnight boundary crossings"
  ✅ "should detect chronological violations per user"
  ✅ "should handle identical timestamps for same user"
  ✅ "should handle timezone inconsistencies in same user data"
}
```

### **Unit Tests: Data Standardization**

#### **Artist/Track Name Cleaning Tests**
```scala
class NameStandardizationTests {
  // Basic Cleaning
  ✅ "should trim leading and trailing whitespace"
  ✅ "should normalize multiple spaces to single space"
  ✅ "should remove tab characters"
  ✅ "should remove newline and carriage return characters"
  ✅ "should handle null values with default replacement"
  ✅ "should handle empty strings with default replacement"
  
  // Unicode and International Characters
  ✅ "should preserve valid Unicode characters (Greek, Cyrillic)"
  ✅ "should normalize Unicode combining characters"
  ✅ "should handle accented characters consistently"
  ✅ "should preserve emoji and special music symbols"
  
  // Length and Safety
  ✅ "should truncate extremely long names to reasonable limit"
  ✅ "should handle names with only special characters"
  ✅ "should remove dangerous control characters"
  ✅ "should handle mixed character encodings"
  
  // Music-Specific Patterns
  ✅ "should standardize 'feat.' vs 'featuring' vs 'ft.'"
  ✅ "should handle remix indicators consistently"
  ✅ "should preserve important punctuation (apostrophes, hyphens)"
  ✅ "should handle case variations consistently"
}
```

#### **Track Identity Resolution Tests**
```scala
class TrackIdentityTests {
  // MBID Priority Logic
  ✅ "should use Track MBID when available"
  ✅ "should fall back to Artist MBID + track name when track MBID missing"
  ✅ "should fall back to name combination when no MBIDs available"
  ✅ "should validate MBID format when present"
  
  // MBID Validation
  ✅ "should accept valid UUID format MBIDs"
  ✅ "should reject malformed MBID strings"
  ✅ "should handle null MBIDs gracefully"
  ✅ "should handle empty string MBIDs"
  
  // Composite Key Generation
  ✅ "should generate consistent track keys for identical data"
  ✅ "should generate different keys for different tracks"
  ✅ "should handle special characters in composite keys"
  ✅ "should create deterministic keys for same input"
  
  // Edge Cases
  ✅ "should handle tracks with MBID but no names"
  ✅ "should handle tracks with names but invalid MBIDs"
  ✅ "should handle completely missing track information"
  ✅ "should preserve enough information for later analysis"
}
```

### **Integration Tests: Pipeline Orchestration**

#### **End-to-End Cleaning Tests**
```scala
class CleaningPipelineTests {
  // Pipeline Flow
  ✅ "should process valid records through all stages"
  ✅ "should reject records at appropriate stage"
  ✅ "should maintain record processing order"
  ✅ "should handle mixed valid/invalid batches"
  
  // Error Handling
  ✅ "should collect and report all validation errors"
  ✅ "should continue processing after recoverable errors"
  ✅ "should stop processing on critical configuration errors"
  ✅ "should provide detailed error context for debugging"
  
  // Performance
  ✅ "should process large datasets within memory limits"
  ✅ "should maintain consistent performance across data sizes"
  ✅ "should handle skewed data distributions efficiently"
  
  // Quality Reporting
  ✅ "should generate comprehensive quality reports"
  ✅ "should track cleaning statistics accurately"
  ✅ "should identify data quality trends"
}
```

### **Property-Based Tests (ScalaCheck)**

#### **Data Integrity Properties**
```scala
class DataCleaningProperties {
  ✅ "cleaned record count should never exceed input count"
  ✅ "rejected records + accepted records should equal input count"
  ✅ "all timestamps in cleaned data should be parseable"
  ✅ "all user IDs in cleaned data should be valid"
  ✅ "track keys should be deterministic for identical inputs"
  ✅ "no cleaned record should have null critical fields"
  ✅ "cleaning should be idempotent (cleaning cleaned data = no change)"
  ✅ "temporal ordering should be preserved within user sessions"
}
```

### **Edge Case Scenario Tests**

#### **Real-World Data Challenges**
```scala
class EdgeCaseScenarioTests {
  // Data Volume Edge Cases
  ✅ "should handle users with no listening history"
  ✅ "should handle users with single track play"
  ✅ "should handle users with extremely high play counts (>100k)"
  ✅ "should handle datasets with no valid records"
  ✅ "should handle datasets with all identical records"
  
  // Character Encoding Challenges (Based on analysis results)
  ✅ "should handle Greek artist names (Μ-Ziq, Έλενα Παπαρίζου)"
  ✅ "should handle Cyrillic artist names (Проверочная Линейка, Ленина Пакет)"
  ✅ "should handle mixed character sets in single record"
  ✅ "should handle corrupted character encoding"
  
  // Timestamp Edge Cases
  ✅ "should handle year 2038 problem (Unix timestamp overflow)"
  ✅ "should handle leap year February 29th timestamps"
  ✅ "should handle timezone transitions (spring forward, fall back)"
  ✅ "should handle historical timezone changes"
  
  // Music Industry Edge Cases
  ✅ "should handle extremely long song titles (>500 characters)"
  ✅ "should handle songs with only punctuation as names"
  ✅ "should handle classical music naming conventions"
  ✅ "should handle podcast episodes vs music tracks"
  ✅ "should handle live performance vs studio recording variants"
}
```

---

## 📊 **Quality Monitoring & Metrics**

### **Critical Quality Metrics (Must Track)**
- **Processing Success Rate**: % of records successfully cleaned vs rejected
- **Critical Field Validity**: % of records with valid user ID and timestamp
- **Track Identity Confidence**: Distribution of MBID vs name-only matching
- **Timestamp Chronology**: % of users with proper chronological ordering
- **Cleaning Effectiveness**: Before/after quality score comparison

### **Quality Thresholds (Based on ADR-005)**
- **Acceptable**: < 5% rejection rate overall
- **Warning**: 5-10% rejection rate (investigate data source)
- **Critical**: > 10% rejection rate (halt processing)

### **Specific Session Analysis Metrics**
- **Session Boundary Integrity**: % of valid timestamp sequences per user
- **Track Matching Confidence**: 
  - High: Track MBID available (target: maintain >85%)
  - Medium: Artist MBID only (target: <10%)  
  - Low: Name matching only (target: <5%)

---

## 🚨 **Error Handling Strategy**

### **Error Classification System**
```scala
sealed trait CleaningError
case class CriticalFieldError(field: String, value: String, reason: String) extends CleaningError
case class StandardizationWarning(field: String, original: String, cleaned: String) extends CleaningError
case class QualityAlert(metric: String, threshold: Double, actual: Double) extends CleaningError
```

### **Recovery Strategies**
- **Critical Errors**: Reject record, log for investigation, continue processing
- **Standardization Issues**: Apply cleaning rules, log transformation, continue processing
- **Quality Warnings**: Accept record, flag for monitoring, continue processing

### **Alerting Triggers**
- **Immediate**: Critical field error rate > 10%
- **Hourly**: Quality degradation trends
- **Daily**: Comprehensive quality reports

---

## 🎯 **Success Criteria**

### **Functional Requirements**
- ✅ All records suitable for session analysis (valid user ID + timestamp)
- ✅ Consistent track identification across 96.9% of data
- ✅ Proper handling of international character sets
- ✅ Deterministic, repeatable cleaning results

### **Performance Requirements**
- ✅ Process 19M records in < 1 minute (cleaning component only)
- ✅ Memory usage < 2GB for cleaning operations
- ✅ Quality report generation < 30 seconds

### **Quality Requirements**
- ✅ Overall rejection rate < 5%
- ✅ Track identity confidence > 95%
- ✅ Zero data corruption during cleaning
- ✅ Complete audit trail of all cleaning operations

---

## 🔗 **Implementation Notes**

### **Integration with Existing Architecture**
- **Aligns with ADR-005**: Hybrid Data Cleaning Approach with tiered validation
- **Supports ADR-001**: Hexagonal Architecture through clean separation of validation concerns
- **Enables ADR-003**: Optimal partitioning by ensuring clean userId fields for repartitioning
- **Complements ADR-007**: Comprehensive testing strategy with extensive edge case coverage

### **Pipeline Context Integration**
- **Data Quality Context**: Primary implementation location for cleaning logic
- **Session Analysis Context**: Consumer of cleaned data with quality guarantees
- **Ranking Context**: Benefits from standardized track identity resolution

### **Monitoring Integration**
- **Quality Metrics**: Seamless integration with existing data quality reporting framework
- **Performance Tracking**: Cache efficiency and processing time monitoring
- **Business Impact**: Track cleaning effectiveness impact on session analysis accuracy

---

*Document Version: 1.0*  
*Last Updated: [Current Date]*  
*Author: Data Engineering Team*  
*Review Cycle: Before each major release*


