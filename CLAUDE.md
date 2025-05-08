# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

clickhouse-filters is a Rust library for generating ClickHouse SQL for pagination, sorting, and advanced filtering. It supports complex AND/OR conditions, case-sensitive/insensitive text searching, array operations, date range filtering, and more.

The library is designed as a counterpart to pg_filters, maintaining API compatibility where possible while optimizing for ClickHouse's specific features and syntax.

## Project Structure

- `src/lib/`: Core library code
  - `mod.rs`: Main library entry point and ClickHouseFilters implementation
  - `filtering.rs`: SQL filtering implementation (FilterCondition, FilterExpression, FilterBuilder)
  - `pagination.rs`: SQL pagination implementation (Pagination, Paginate)
  - `sorting.rs`: SQL sorting implementation (SortOrder, SortedColumn, Sorting)
- `tests/`: Test suite
  - `unit/`: Unit tests for individual components
  - `integration/`: Integration tests with ClickHouse test containers

## Build and Test Commands

```bash
# Build the project
cargo build

# Build for release
cargo build --release

# Run all tests (unit and integration)
cargo test

# Run unit tests only
cargo test unit::

# Run only doctests
cargo test --doc

# Run specific test file
cargo test filtering_test
cargo test integration_test

# Run a specific test
cargo test test_logical_filters

# Run tests with output
cargo test -- --nocapture
```

## Implementation Notes

The implementation is now complete with:

1. **Data Structure Support**: Supports all common ClickHouse column types including String, Int/UInt, Float, Date/DateTime, UUID, Boolean, Array types, and JSON.

2. **Advanced Filtering**: 
   - Comprehensive support for operators (=, !=, >, >=, <, <=, LIKE, NOT LIKE, IN, NOT IN, IS NULL, IS NOT NULL)
   - ClickHouse-specific operators (ARRAY CONTAINS, ARRAY HAS)
   - Date-specific operators (DATE_ONLY, DATE_RANGE, RELATIVE)
   - Complex AND/OR logic with nested groups

3. **JSON Filtering Interface**: Support for JSON-based filtering for API integrations

4. **Type-aware Condition Generation**: Automatically converts values to the correct types based on column definitions

5. **Unit Tests**: Comprehensive test coverage for all components

6. **Integration Tests**: Test structure is in place but needs updating to work with latest ClickHouse client API

Note: The integration tests currently don't work due to API compatibility issues with the latest ClickHouse client. They need to be updated to match the current API, which has changed methods like `Client::from_options` and `execute()`.

## Code Quality Commands

```bash
# Format code
cargo fmt --all

# Run linter
cargo clippy

# Run both format and clippy (using fix.sh)
./fix.sh
```

## Key Components

### ColumnDef
Defines the column types supported by the library (String, UInt32, Boolean, DateTime, UUID, etc.)

### FilterCondition
Represents a single filter condition (column, operator, value)

### FilterExpression
Represents either a single condition or a group of conditions with a logical operator (AND/OR)

### FilterBuilder
Builds complex SQL WHERE clauses from filter expressions

### PaginationOptions and Paginate
Handles pagination with current page, per page, and total records

### SortedColumn and Sorting
Handles SQL ORDER BY clauses with multiple sort columns

### ClickHouseFilters
Main entry point that combines filtering, sorting, and pagination

## Common Development Patterns

1. Define column definitions with appropriate types:
   ```rust
   let mut columns = HashMap::new();
   columns.insert("name", ColumnDef::String("name"));
   columns.insert("age", ColumnDef::UInt32("age"));
   ```

2. Create filter expressions:
   ```rust
   let name_filter = FilterExpression::Condition(FilterCondition::StringValue {
       column: "name".to_string(),
       operator: FilterOperator::Like,
       value: Some("%John%".to_string()),
   });
   ```

3. Create pagination options:
   ```rust
   let pagination = PaginationOptions {
       current_page: 1,
       per_page: 10,
       per_page_limit: 10,
       total_records: 100,
   };
   ```

4. Combine everything into ClickHouseFilters:
   ```rust
   let filters = ClickHouseFilters::new(
       Some(pagination),
       vec![SortedColumn::new("name", "asc")],
       Some(FilteringOptions::new(vec![name_filter], columns.clone())),
       columns
   )?;
   ```

5. Generate SQL:
   ```rust
   let sql = filters.sql()?;
   ```

## ClickHouse Specifics

When developing, keep in mind the differences between PostgreSQL and ClickHouse:

1. Case sensitivity in ClickHouse differs from PostgreSQL
2. ClickHouse array handling uses functions like `hasAll`, `has` instead of operators
3. ClickHouse SQL syntax has slight differences in some areas
4. ClickHouse doesn't support all PostgreSQL operators and functions
5. Performance characteristics are different due to column-based vs row-based storage