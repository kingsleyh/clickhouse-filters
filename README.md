
# ClickHouse Filters

[![License](https://img.shields.io/badge/license-MIT%2FApache-blue.svg)](https://github.com/kingsleyh/clickhouse-filters#license)
[![Docs](https://docs.rs/clickhouse-filters/badge.svg)](https://docs.rs/clickhouse-filters/latest/clickhouse_filters/)
[![Test](https://github.com/kingsleyh/clickhouse-filters/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/kingsleyh/clickhouse-filters/actions/workflows/ci.yml)
[![Crates](https://img.shields.io/crates/v/clickhouse-filters.svg)](https://crates.io/crates/clickhouse-filters)

A powerful Rust helper to generate ClickHouse SQL for pagination, sorting, and advanced filtering with support for complex AND/OR conditions.

## Overview

`clickhouse-filters` is designed to work with ClickHouse databases while maintaining API compatibility with `pg_filters`. This crate helps you build SQL clauses for ClickHouse, handling:

- Complex filtering with AND/OR conditions
- Pagination with limit and offset
- Multi-column sorting
- Type-aware filtering for various ClickHouse data types
- Special ClickHouse-specific features and optimizations

## Usage

### Column Definitions

First, define your column types:

```rust
use std::collections::HashMap;
use clickhouse_filters::ColumnDef;

fn setup_columns() -> HashMap<&'static str, ColumnDef> {
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("age", ColumnDef::UInt32("age"));
    columns.insert("email", ColumnDef::String("email"));
    columns.insert("active", ColumnDef::Boolean("active"));
    columns.insert("created_at", ColumnDef::DateTime("created_at"));
    columns.insert("id", ColumnDef::UUID("id"));
    columns
}
```

### Basic Filtering

```rust
use clickhouse_filters::{ClickHouseFilters, PaginationOptions, FilteringOptions, ColumnDef};
use clickhouse_filters::filtering::{FilterCondition, FilterExpression, FilterOperator};
use clickhouse_filters::sorting::SortedColumn;

let columns = setup_columns();

// Create simple conditions
let name_condition = FilterExpression::Condition(FilterCondition::StringValue {
    column: "name".to_string(),
    operator: FilterOperator::Equal,
    value: Some("John".to_string()),
});

let age_condition = FilterExpression::Condition(FilterCondition::UInt32Value {
    column: "age".to_string(),
    operator: FilterOperator::GreaterThan,
    value: Some(18),
});

let filters = ClickHouseFilters::new(
    Some(PaginationOptions {
        current_page: 1,
        per_page: 10,
        per_page_limit: 10,
        total_records: 1000,
    }),
    vec![
        SortedColumn::new("age", "desc"),
        SortedColumn::new("name", "asc"),
    ],
    Some(FilteringOptions::new(
        vec![name_condition, age_condition],
        columns.clone(),
    )),
    columns,
)?;

let sql = filters.sql()?;
// Results in: WHERE (lower(name) = lower('John') AND age > 18) ORDER BY age DESC, name ASC LIMIT 10 OFFSET 0
```

### Complex Filtering with AND/OR Logic

```rust
use clickhouse_filters::filtering::{FilterCondition, FilterExpression, FilterOperator};

// Create individual conditions
let name_condition = FilterCondition::string("name", FilterOperator::Like, Some("%John%"));
let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(25));
let active_condition = FilterCondition::boolean("active", FilterOperator::Equal, Some(true));

// Combine with AND/OR logic
let age_and_active = FilterExpression::and(vec![
    FilterExpression::Condition(age_condition),
    FilterExpression::Condition(active_condition),
]);

// Finally combine everything with OR
let complex_filter = FilterExpression::or(vec![
    FilterExpression::Condition(name_condition),
    age_and_active,
]);

// Use in FilteringOptions
let filtering = FilteringOptions::new(vec![complex_filter], columns.clone());
```

### JSON-based Filtering

```rust
use clickhouse_filters::filtering::JsonFilter;

// Create JSON filters for the API
let json_filters = vec![
    JsonFilter {
        n: "age".to_string(),      // column name
        f: ">".to_string(),        // operator
        v: "25".to_string(),       // value
        c: Some("AND".to_string()), // connector
    },
    JsonFilter {
        n: "active".to_string(),
        f: "=".to_string(),
        v: "1".to_string(),
        c: None,
    },
];

// Convert to FilteringOptions
let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone())?;
```

## Testing

Unit tests can be run with `cargo test`. Currently, integration tests require a running ClickHouse instance and need to be updated to work with the latest ClickHouse client API.

## Compatibility with pg_filters

This library maintains API compatibility with `pg_filters` where possible, allowing for easy transition between PostgreSQL and ClickHouse implementations. There are some ClickHouse-specific features and optimizations that differ from the PostgreSQL implementation.

## License

Licensed under either of these:
- MIT ([https://opensource.org/licenses/MIT](https://opensource.org/licenses/MIT))
- Apache-2.0 ([https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0))