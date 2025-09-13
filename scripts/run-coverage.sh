#!/bin/bash

# LastFM Session Analyzer - Test Coverage Analysis Script
# This script runs the test suite with coverage collection and generates reports

set -e

echo "🧪 LastFM Session Analyzer - Coverage Analysis"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if SBT is available
if ! command -v sbt &> /dev/null; then
    print_error "SBT is not installed or not in PATH"
    exit 1
fi

print_status "Starting coverage analysis..."

# Clean previous build artifacts and coverage data
print_status "Cleaning previous build artifacts..."
sbt clean

# Enable coverage and run tests
print_status "Running tests with coverage collection..."
print_status "This may take a few minutes due to coverage instrumentation..."

if sbt coverage test; then
    print_success "Tests completed successfully"
else
    print_warning "Some tests failed, but continuing with coverage report generation"
fi

# Generate coverage reports
print_status "Generating coverage reports..."
sbt coverageReport

# Check if reports were generated successfully
COVERAGE_HTML="target/scala-2.13/scoverage-report/index.html"
COVERAGE_XML="target/scala-2.13/scoverage-report/scoverage.xml"

if [ -f "$COVERAGE_HTML" ]; then
    print_success "Coverage reports generated successfully!"
    echo ""
    echo "📊 Coverage Reports Available:"
    echo "  HTML Report: $COVERAGE_HTML"
    echo "  XML Report:  $COVERAGE_XML"
    echo ""
    
    # Try to open the HTML report if on macOS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        print_status "Opening coverage report in browser..."
        open "$COVERAGE_HTML"
    elif command -v xdg-open &> /dev/null; then
        print_status "Opening coverage report in browser..."
        xdg-open "$COVERAGE_HTML"
    else
        print_status "Open the following file in your browser to view the report:"
        echo "  file://$(pwd)/$COVERAGE_HTML"
    fi
    
    # Extract coverage summary from console output
    print_status "Coverage Summary:"
    echo "  Check the console output above for statement and branch coverage percentages"
    
else
    print_error "Coverage report generation failed"
    exit 1
fi

# Provide next steps
echo ""
print_success "Coverage analysis complete!"
echo ""
echo "📋 Next Steps:"
echo "  1. Review the HTML coverage report for detailed line-by-line analysis"
echo "  2. Focus on uncovered code in core business logic (domain package)"
echo "  3. Add tests for uncovered branches and statements"
echo "  4. Run this script again to track improvement"
echo ""
echo "💡 Development Tips:"
echo "  • Use 'sbt coverage test' for coverage during development"
echo "  • Use 'sbt coverageOff test' for faster test runs without coverage"
echo "  • Coverage targets: 80% statement, 70% branch coverage"
