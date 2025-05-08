
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
- JSON filters for API-friendly filtering

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
clickhouse-filters = "0.1.0"
```

## Supported Column Types

ClickHouse Filters supports a wide range of ClickHouse data types:

### String Types
- `String` / `FixedString`

### Numeric Types
- Integers: `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256`, `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`
- Floating Points: `Float32`, `Float64`

### Date and Time Types
- `Date`, `Date32`
- `DateTime`, `DateTime64`

### Other Basic Types
- `Boolean`
- `UUID`
- `Decimal`

### Complex Types
- Arrays: `ArrayString`, `ArrayUInt8`, `ArrayUInt16`, etc.
- `JSON`
- `Enum8`, `Enum16`
- Network Types: `IPv4`, `IPv6`

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
    columns.insert("active", ColumnDef::UInt8("active"));
    columns.insert("created_at", ColumnDef::DateTime("created_at"));
    columns.insert("id", ColumnDef::UUID("id"));
    columns.insert("tags", ColumnDef::ArrayString("tags"));
    columns.insert("user_data", ColumnDef::JSON("user_data"));
    columns
}
```

### Basic Filtering

```rust
use clickhouse_filters::{ClickHouseFilters, FilteringOptions, ColumnDef};
use clickhouse_filters::filtering::{FilterCondition, FilterExpression, FilterOperator};

let columns = setup_columns();

// Using helper constructors
let name_filter = FilterCondition::string("name", FilterOperator::Equal, Some("John"));
let age_filter = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(18));

// Create filter expressions
let name_expr = FilterExpression::Condition(name_filter);
let age_expr = FilterExpression::Condition(age_filter);

// Create filtering options
let filtering = FilteringOptions::new(vec![name_expr, age_expr], columns.clone());

// Create filters without pagination or sorting
let filters = ClickHouseFilters::new(
    None,
    vec![],
    Some(filtering),
    columns,
)?;

let sql = filters.sql()?;
// Results in: WHERE (lower(name) = lower('John') AND age > 18)
```

### Supported Filter Operators

ClickHouse Filters supports a rich set of operators:

```rust
// Comparison operators
FilterOperator::Equal              // =
FilterOperator::NotEqual           // !=
FilterOperator::GreaterThan        // >
FilterOperator::GreaterThanOrEqual // >=
FilterOperator::LessThan           // <
FilterOperator::LessThanOrEqual    // <=

// String operators
FilterOperator::Like               // LIKE
FilterOperator::NotLike            // NOT LIKE
FilterOperator::StartsWith         // LIKE 'value%'
FilterOperator::EndsWith           // LIKE '%value'

// Collection operators
FilterOperator::In                 // IN (...)
FilterOperator::NotIn              // NOT IN (...)

// NULL checks
FilterOperator::IsNull             // IS NULL
FilterOperator::IsNotNull          // IS NOT NULL

// Array operators (ClickHouse specific)
FilterOperator::ArrayContains      // hasAll
FilterOperator::ArrayHas           // has
FilterOperator::ArrayAll           // ALL
FilterOperator::ArrayAny           // ANY

// Date operators
FilterOperator::DateEqual          // Exact date match
FilterOperator::DateRange          // Date between range
FilterOperator::RelativeDate       // Relative date expressions
```

### Complex Filtering with AND/OR Logic

```rust
use clickhouse_filters::filtering::{FilterCondition, FilterExpression, FilterOperator};

// Create individual conditions
let name_condition = FilterCondition::string("name", FilterOperator::Like, Some("%John%"));
let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(25));
let active_condition = FilterCondition::uint8("active", FilterOperator::Equal, Some(1));

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

// Generated SQL will be: WHERE (lower(name) LIKE lower('%John%') OR (age > 25 AND active = 1))
```

### Array Filtering

ClickHouse has specific array functions that this library supports:

```rust
// Filter for array containing a specific value
let tags_filter = FilterCondition::array_has("tags", "developer");
// Generates: has(tags, 'developer')

// Filter for array containing all specified values
let tags_filter = FilterCondition::array_contains("tags", "developer,rust");
// Generates: hasAll(tags, array['developer', 'rust'])
```

### JSON Filtering

You can filter on JSON fields using path notation:

```rust
// Simple JSON field equality
let json_filter = FilterCondition::json(
    "user_data", 
    FilterOperator::Equal, 
    Some("premium"), 
    Some("subscription.type")
);
// Generates: JSONExtractString(user_data, 'subscription.type') = 'premium'
```

### JSON-based API Filtering

For API-friendly filtering, use the JsonFilter structure:

```rust
use clickhouse_filters::filtering::JsonFilter;

// Create JSON filters that can be easily serialized/deserialized
let json_filters = vec![
    JsonFilter {
        n: "age".to_string(),      // column name
        f: ">".to_string(),        // operator
        v: "25".to_string(),       // value
        c: Some("AND".to_string()), // connector (AND/OR)
    },
    JsonFilter {
        n: "active".to_string(),
        f: "=".to_string(),
        v: "1".to_string(),
        c: Some("OR".to_string()),
    },
    JsonFilter {
        n: "tags".to_string(),
        f: "ARRAY HAS".to_string(),
        v: "developer".to_string(),
        c: None,
    },
];

// Convert to FilteringOptions
let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone())?;
// Generated SQL: WHERE (age > 25 AND active = 1 OR has(tags, 'developer'))
```

Supported operators in JsonFilter format:

| Operator       | Description                        |
|----------------|------------------------------------|
| `=`            | Equal                              |
| `!=`           | Not Equal                          |
| `>`            | Greater Than                       |
| `>=`           | Greater Than or Equal              |
| `<`            | Less Than                          |
| `<=`           | Less Than or Equal                 |
| `LIKE`         | Like pattern matching              |
| `NOT LIKE`     | Not like pattern matching          |
| `IN`           | In a list of values (comma-separated) |
| `NOT IN`       | Not in a list of values            |
| `IS NULL`      | Is null check                      |
| `IS NOT NULL`  | Is not null check                  |
| `STARTS WITH`  | Starts with pattern                |
| `ENDS WITH`    | Ends with pattern                  |
| `ARRAY HAS`    | Array contains value               |
| `ARRAY CONTAINS` | Array contains all values        |
| `ARRAY ALL`    | Check if all elements match a condition |
| `ARRAY ANY`    | Check if any elements match a condition |
| `DATE_ONLY`    | Match date part only               |
| `DATE_RANGE`   | Date within range (comma-separated start,end) |
| `RELATIVE`     | Relative date expression           |

### Pagination

To implement pagination:

```rust
use clickhouse_filters::PaginationOptions;

// Create pagination options
let pagination = PaginationOptions::new(
    2,     // current_page (1-based)
    15,    // per_page (items per page)
    50,    // per_page_limit (maximum allowed per_page)
    100,   // total_records (total count)
);

// Use with ClickHouseFilters
let filters = ClickHouseFilters::new(
    Some(pagination),
    vec![],
    None,
    columns,
)?;

let sql = filters.sql()?;
// Results in: LIMIT 15 OFFSET 15
```

The `PaginationOptions` struct will automatically calculate:
- Page boundaries (preventing out-of-range pages)
- Proper offset values
- Previous and next page numbers
- Total page count

### Sorting

To implement sorting:

```rust
use clickhouse_filters::sorting::SortedColumn;

// Create sorting options
let sorting = vec![
    SortedColumn::new("age", "desc"),   // Sort by age descending
    SortedColumn::new("name", "asc"),   // Then by name ascending
];

// Use with ClickHouseFilters
let filters = ClickHouseFilters::new(
    None,
    sorting,
    None,
    columns,
)?;

let sql = filters.sql()?;
// Results in: ORDER BY age DESC, name ASC
```

### Combining Everything

```rust
use clickhouse_filters::{ClickHouseFilters, PaginationOptions, FilteringOptions};
use clickhouse_filters::filtering::{FilterCondition, FilterExpression, FilterOperator};
use clickhouse_filters::sorting::SortedColumn;

let columns = setup_columns();

// Create a filter condition
let filter_expr = FilterExpression::Condition(FilterCondition::string(
    "name", 
    FilterOperator::StartsWith, 
    Some("J")
));

// Create filters with pagination, sorting, and filtering
let filters = ClickHouseFilters::new(
    Some(PaginationOptions::new(1, 10, 50, 1000)),
    vec![SortedColumn::new("created_at", "desc")],
    Some(FilteringOptions::new(
        vec![filter_expr],
        columns.clone(),
    )),
    columns,
)?;

// Generate the full SQL for a query
let sql = filters.query_sql("my_database", "users_table", &["id", "name", "email"])?;
// Results in: SELECT id, name, email FROM my_database.users_table WHERE lower(name) LIKE lower('J%') ORDER BY created_at DESC LIMIT 10 OFFSET 0
```

## Complete Example with ClickHouse Client

```rust
use clickhouse::Client;
use clickhouse_filters::{ClickHouseFilters, FilteringOptions};
use clickhouse_filters::filtering::{FilterCondition, FilterOperator, JsonFilter};
use serde::Deserialize;

#[derive(Debug, Deserialize, clickhouse::Row)]
struct User {
    id: String,
    name: String,
    age: u32,
}

async fn list_users(client: &Client, json_filters: Vec<JsonFilter>) -> Result<Vec<User>, Box<dyn std::error::Error>> {
    // Define column types
    let mut columns = HashMap::new();
    columns.insert("id", ColumnDef::UUID("id"));
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("age", ColumnDef::UInt32("age"));
    
    // Create filtering options from JSON
    let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone())?;
    
    // Create combined filters
    let filters = ClickHouseFilters::new(
        Some(PaginationOptions::new(1, 10, 50, 0)), // page 1, 10 per page
        vec![SortedColumn::new("name", "asc")],     // sort by name
        filtering,                                  // add the filters
        columns,
    )?;
    
    // Generate SQL for the query
    let sql = filters.query_sql("my_database", "users", &["id", "name", "age"])?;
    
    // Execute query
    let result = client.query(&sql).fetch_all::<User>().await?;
    
    Ok(result)
}
```

## Testing

Unit tests can be run with `cargo test`. Integration tests require a running ClickHouse instance via Docker containers and will be automatically set up when running `cargo test --test mod`.

## Compatibility with pg_filters

This library maintains API compatibility with `pg_filters` where possible, allowing for easy transition between PostgreSQL and ClickHouse implementations. There are some ClickHouse-specific features and optimizations that differ from the PostgreSQL implementation, such as:

- ClickHouse-specific array functions (`has`, `hasAll`)
- Different JSON path extraction using `JSONExtractString`
- Additional ClickHouse data types
- ClickHouse-specific date/time handling

## License

Licensed under either of these:
- MIT ([https://opensource.org/licenses/MIT](https://opensource.org/licenses/MIT))
- Apache-2.0 ([https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0))