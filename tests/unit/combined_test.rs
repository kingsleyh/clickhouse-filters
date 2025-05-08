//! Unit tests for the combined functionality (filtering, pagination, sorting)

use clickhouse_filters::filtering::{
    FilterCondition, FilterExpression, FilterOperator, JsonFilter,
};
use clickhouse_filters::sorting::SortedColumn;
use clickhouse_filters::{ClickHouseFilters, ColumnDef, FilteringOptions, PaginationOptions};
use std::collections::HashMap;

#[test]
fn test_complete_filtering() {
    // Create column definitions
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("age", ColumnDef::UInt32("age"));
    columns.insert("active", ColumnDef::Boolean("active"));
    columns.insert("created_at", ColumnDef::DateTime("created_at"));

    // Create filtering options
    let filter_expr = FilterExpression::Condition(FilterCondition::string(
        "name",
        FilterOperator::Equal,
        Some("John Doe"),
    ));

    let filtering = FilteringOptions::new(vec![filter_expr], columns.clone());

    // Create pagination options
    let pagination = PaginationOptions::new(1, 10, 100, 1000);

    // Create sorting
    let sorting = vec![
        SortedColumn::new("age", "desc"),
        SortedColumn::new("name", "asc"),
    ];

    // Create ClickHouseFilters
    let filters =
        ClickHouseFilters::new(Some(pagination), sorting, Some(filtering), columns).unwrap();

    // Generate SQL
    let sql = filters.sql().unwrap();
    assert!(sql.contains("name =") || sql.contains("lower(name)"));
    assert!(sql.contains("ORDER BY"));
    assert!(sql.contains("LIMIT 10 OFFSET 0"));

    // Generate count SQL
    let count_sql = filters.count_sql("my_db", "users").unwrap();
    assert!(count_sql.contains("SELECT COUNT(*) FROM my_db.users"));
    assert!(count_sql.contains("WHERE"));

    // Generate query SQL
    let query_sql = filters
        .query_sql("my_db", "users", &["name", "age"])
        .unwrap();
    assert!(query_sql.contains("SELECT name, age FROM my_db.users"));
    assert!(query_sql.contains("WHERE"));
    assert!(query_sql.contains("ORDER BY"));
    assert!(query_sql.contains("LIMIT 10 OFFSET 0"));
}

#[test]
fn test_json_filters() {
    // Create column definitions
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("age", ColumnDef::UInt32("age"));
    columns.insert("active", ColumnDef::Boolean("active"));

    // Create JSON filters
    let json_filters = vec![
        JsonFilter {
            n: "name".to_string(),
            f: "LIKE".to_string(),
            v: "John%".to_string(),
            c: Some("AND".to_string()),
        },
        JsonFilter {
            n: "age".to_string(),
            f: ">".to_string(),
            v: "30".to_string(),
            c: None,
        },
    ];

    // Create filtering options from JSON
    let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone()).unwrap();

    // Create ClickHouseFilters
    let filters = ClickHouseFilters::new(None, vec![], filtering, columns).unwrap();

    // Generate SQL
    let sql = filters.sql().unwrap();
    assert!(sql.contains("name") && sql.contains("LIKE"));
    assert!(sql.contains("age") && sql.contains(">"));
}

#[test]
fn test_combined_filters() {
    // Create column definitions
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("age", ColumnDef::UInt32("age"));
    columns.insert("tags", ColumnDef::ArrayString("tags"));

    // Create complex filter expression
    let name_condition = FilterCondition::string("name", FilterOperator::Like, Some("%Smith%"));

    let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(25));

    let tags_condition = FilterCondition::array_has("tags", "premium");

    // Combine with AND/OR logic
    let age_and_tags = FilterExpression::and(vec![
        FilterExpression::Condition(age_condition),
        FilterExpression::Condition(tags_condition),
    ]);

    let combined_expr = FilterExpression::or(vec![
        FilterExpression::Condition(name_condition),
        age_and_tags,
    ]);

    let filtering = FilteringOptions::new(vec![combined_expr], columns.clone());

    // Create ClickHouseFilters with sorting and pagination
    let filters = ClickHouseFilters::new(
        Some(PaginationOptions::new(2, 15, 50, 100)),
        vec![SortedColumn::new("name", "asc")],
        Some(filtering),
        columns,
    )
    .unwrap();

    // Generate SQL
    let sql = filters.sql().unwrap();

    // Check for OR logic
    assert!(sql.contains("OR"));

    // Check for AND logic
    assert!(sql.contains("AND"));

    // Check for array operations
    assert!(sql.contains("has(tags"));

    // Check for correct pagination (page 2 with 15 per page = offset 15)
    assert!(sql.contains("LIMIT 15 OFFSET 15"));
}
