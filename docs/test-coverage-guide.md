# Test Coverage Guide

## Overview
This project uses the scoverage plugin to measure code coverage. The plugin is configured to track both statement and branch coverage with reasonable thresholds aligned with the project's TDD approach.

## Configuration

### Plugin Setup
- **Plugin**: `sbt-scoverage` version 2.0.9
- **Location**: `project/plugins.sbt`

### Coverage Settings
- **Statement Coverage Target**: 80% minimum
- **Branch Coverage Target**: 70% minimum  
- **Fail on Minimum**: Currently disabled (set to `false`) until baseline is established

### Excluded Packages
The following packages are excluded from coverage to focus on business logic:
- Infrastructure adapters (`*.infrastructure.*`)
- Configuration classes (`*.config.*`) 
- Main application entry points (`*Main*`)

## Usage

### Run Tests with Coverage
```bash
# Clean previous coverage data
sbt clean

# Run tests with coverage collection
sbt coverage test

# Generate coverage report
sbt coverageReport
```

### Coverage Reports
After running `sbt coverageReport`, reports are generated in:
- **HTML Report**: `target/scala-2.13/scoverage-report/index.html`
- **XML Report**: `target/scala-2.13/scoverage-report/scoverage.xml`
- **Console Summary**: Displayed after report generation

### Coverage Commands
```bash
# Enable coverage for this session
sbt coverage

# Run tests with coverage
sbt test

# Generate HTML and XML reports
sbt coverageReport

# Generate aggregated coverage report (if using sub-projects)
sbt coverageAggregate

# Turn off coverage for faster test runs
sbt coverageOff test
```

## Current Test Status
Based on recent test runs:
- **Total Test Cases**: ~485+ across 37 test suites
- **File Coverage Ratio**: 89% (40 test files / 45 source files)
- **Passing Tests**: Majority passing with excellent domain coverage
- **Failing Tests**: Some pipeline and orchestration tests need attention

### Well-Tested Components
- **SessionCalculator**: 24 comprehensive tests
- **Domain Models**: UserSession (16), ListenEvent (20), Track (11)
- **Data Validation**: LastFmDataValidation (26 tests)
- **Infrastructure**: SparkDataRepository (22 tests)

### Areas Needing Attention
- `DataCleaningSimpleSpec`: 5/5 failing
- `MainOrchestrationSpec`: 5/11 failing
- `DataCleaningPipelineSpec`: 10/13 failing

## Coverage Targets

### Phase 1 Goals
- [ ] Establish baseline coverage metrics
- [ ] Fix failing tests to achieve clean test suite
- [ ] Reach 80% statement coverage on core domain logic

### Phase 2 Goals  
- [ ] Achieve 80%+ statement coverage overall
- [ ] Achieve 70%+ branch coverage
- [ ] Enable `coverageFailOnMinimum := true`

### Phase 3 Goals
- [ ] Maintain 85%+ coverage as codebase evolves
- [ ] Add mutation testing for critical algorithms
- [ ] Integrate coverage into CI/CD pipeline

## Best Practices

### What to Cover
- **Domain Logic**: All business rules and calculations
- **Data Validation**: Input validation and data quality rules
- **Session Analysis**: Core session boundary and metrics calculations
- **Error Handling**: Exception paths and edge cases

### What to Exclude
- **Configuration Boilerplate**: Simple getters/setters
- **Infrastructure Adapters**: Database/file system adapters
- **Main Entry Points**: Application bootstrapping code
- **Generated Code**: Auto-generated classes

### Writing Coverage-Friendly Tests
1. **Test Behavior, Not Implementation**: Focus on what the code does
2. **Cover Edge Cases**: Null inputs, empty collections, boundary conditions
3. **Test Error Paths**: Exception handling and validation failures
4. **Use Data Builders**: Create reusable test data factories

## Integration with TDD

Following the project's TDD approach:

1. **Red**: Write failing test for new functionality
2. **Green**: Write minimal code to make test pass
3. **Refactor**: Improve code while maintaining coverage
4. **Verify**: Run `sbt coverage test coverageReport` to confirm coverage

This ensures that coverage naturally reaches high levels through the TDD cycle rather than being an afterthought.

## Troubleshooting

### Common Issues
- **Low Coverage**: Often indicates missing edge case tests
- **Coverage Not Generated**: Ensure `sbt coverage` was run before tests
- **Slow Test Runs**: Use `sbt coverageOff` for development, coverage for CI

### Performance Impact
- Coverage collection adds ~10-15% overhead to test execution
- Use coverage selectively during development
- Always run coverage in CI/CD pipelines
