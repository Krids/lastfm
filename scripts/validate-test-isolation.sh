#!/bin/bash

# Test Isolation Validation Script
# 
# This script performs comprehensive validation of test isolation to prevent
# production data contamination. It should be run before deployments and
# can be integrated into CI/CD pipelines.
#
# Author: Felipe Lana Machado
# Version: 1.0.0

set -e  # Exit on any error

echo "🔍 LastFM Session Analyzer - Test Isolation Validation"
echo "======================================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Validation results
VALIDATION_ERRORS=0

# Function to report validation results
report_result() {
    local test_name="$1"
    local status="$2"
    local message="$3"
    
    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}✅ PASS${NC}: $test_name"
        [ -n "$message" ] && echo -e "   $message"
    elif [ "$status" = "WARN" ]; then
        echo -e "${YELLOW}⚠️  WARN${NC}: $test_name"
        [ -n "$message" ] && echo -e "   $message"
    else
        echo -e "${RED}❌ FAIL${NC}: $test_name"
        [ -n "$message" ] && echo -e "   $message"
        VALIDATION_ERRORS=$((VALIDATION_ERRORS + 1))
    fi
}

echo -e "${BLUE}Phase 1: Directory Structure Validation${NC}"
echo "----------------------------------------"

# Check if test directories exist
if [ -d "data/test" ]; then
    report_result "Test directory structure" "PASS" "data/test/ directory exists"
    
    # Check required subdirectories
    for subdir in bronze silver gold results; do
        if [ -d "data/test/$subdir" ]; then
            report_result "Test $subdir directory" "PASS"
        else
            report_result "Test $subdir directory" "WARN" "Missing - will be created automatically"
        fi
    done
else
    report_result "Test directory structure" "FAIL" "data/test/ directory missing - test isolation not configured"
fi

# Check production directories exist
if [ -d "data/output" ]; then
    report_result "Production directory structure" "PASS" "data/output/ directory exists"
else
    report_result "Production directory structure" "WARN" "data/output/ directory missing - may be normal in test-only environments"
fi

echo ""
echo -e "${BLUE}Phase 2: Production Contamination Detection${NC}"
echo "--------------------------------------------"

# Check for test artifacts in production directories
CONTAMINATION_FOUND=false
if [ -d "data/output" ]; then
    for layer in bronze silver gold results; do
        if [ -d "data/output/$layer" ]; then
            # Look for test artifacts
            TEST_FILES=$(find "data/output/$layer" -name "*test*" -o -name "*temp-*" -o -name "*tmp-*" -o -name "*spec*" -o -name "*fixture*" 2>/dev/null || true)
            
            if [ -n "$TEST_FILES" ]; then
                report_result "Production $layer contamination check" "FAIL" "Test artifacts found:\n$(echo "$TEST_FILES" | sed 's/^/     /')"
                CONTAMINATION_FOUND=true
            else
                report_result "Production $layer contamination check" "PASS"
            fi
        else
            report_result "Production $layer contamination check" "PASS" "Directory doesn't exist"
        fi
    done
else
    report_result "Production contamination check" "PASS" "No production directories to check"
fi

echo ""
echo -e "${BLUE}Phase 3: Configuration File Validation${NC}"
echo "----------------------------------------"

# Check test configuration
if [ -f "src/test/resources/application.conf" ]; then
    if grep -q "data.output.base.*data/test" "src/test/resources/application.conf"; then
        report_result "Test configuration isolation" "PASS" "Test config uses data/test paths"
    else
        report_result "Test configuration isolation" "FAIL" "Test config not using isolated paths"
    fi
else
    report_result "Test configuration file" "FAIL" "src/test/resources/application.conf not found"
fi

# Check production configuration
if [ -f "src/main/resources/application.conf" ]; then
    if grep -q "data.output.base.*data/output" "src/main/resources/application.conf"; then
        report_result "Production configuration isolation" "PASS" "Production config uses data/output paths"
    else
        report_result "Production configuration paths" "WARN" "Production config may use non-standard paths"
    fi
else
    report_result "Production configuration file" "FAIL" "src/main/resources/application.conf not found"
fi

echo ""
echo -e "${BLUE}Phase 4: GitIgnore Protection Validation${NC}"
echo "---------------------------------------------"

if [ -f ".gitignore" ]; then
    if grep -q "data/test/" ".gitignore"; then
        report_result "Test directory gitignore" "PASS" "data/test/ properly ignored"
    else
        report_result "Test directory gitignore" "FAIL" "data/test/ not properly ignored"
    fi
    
    if grep -q "data/output/.*/test-\*" ".gitignore"; then
        report_result "Production contamination protection" "PASS" "Test artifacts in production directories ignored"
    else
        report_result "Production contamination protection" "WARN" "No protection against test artifacts in production"
    fi
else
    report_result "GitIgnore file" "FAIL" ".gitignore not found"
fi

echo ""
echo -e "${BLUE}Phase 5: Test Safety Implementation Check${NC}"
echo "----------------------------------------------"

# Check for TestConfiguration class
if [ -f "src/test/scala/com/lastfm/sessions/testutil/TestConfiguration.scala" ]; then
    report_result "TestConfiguration implementation" "PASS" "Test safety framework found"
else
    report_result "TestConfiguration implementation" "FAIL" "Test safety framework not implemented"
fi

# Check for BaseTestSpec
if [ -f "src/test/scala/com/lastfm/sessions/testutil/BaseTestSpec.scala" ]; then
    report_result "BaseTestSpec implementation" "PASS" "Base test class with safety validation found"
else
    report_result "BaseTestSpec implementation" "FAIL" "Base test safety class not implemented"
fi

echo ""
echo -e "${BLUE}Validation Summary${NC}"
echo "=================="

if [ $VALIDATION_ERRORS -eq 0 ]; then
    echo -e "${GREEN}🎉 VALIDATION SUCCESSFUL${NC}"
    echo "All test isolation checks passed. Environment is safe for testing and deployment."
    exit 0
else
    echo -e "${RED}🚨 VALIDATION FAILED${NC}"
    echo "Found $VALIDATION_ERRORS critical issues that must be resolved before deployment."
    echo ""
    echo "Recommended actions:"
    echo "1. Fix all FAIL issues listed above"
    echo "2. Clean any contaminated production directories"
    echo "3. Ensure all tests use TestConfiguration.testConfig()"
    echo "4. Re-run this validation script"
    exit 1
fi
