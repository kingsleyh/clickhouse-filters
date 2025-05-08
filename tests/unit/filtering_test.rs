use clickhouse_filters::{
    filtering::{ColumnTypeInfo, FilterCondition, FilterExpression, FilterOperator},
    ColumnDef, FilteringOptions,
};
use std::collections::HashMap;

#[test]
fn test_basic_string_filter() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));

    // Create a simple filter for name = "John Smith"
    let filter_expr = FilterExpression::Condition(FilterCondition::string(
        "name",
        FilterOperator::Equal,
        Some("John Smith"),
    ));

    let filtering = FilteringOptions::new(vec![filter_expr], columns);

    // Verify the SQL output
    assert_eq!(
        filtering.to_sql().unwrap(),
        " WHERE lower(name) = lower('John Smith')"
    );
}

#[test]
fn test_numeric_filter() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("age", ColumnDef::UInt32("age"));

    // Create a filter for age > 25
    let filter_expr = FilterExpression::Condition(FilterCondition::uint32(
        "age",
        FilterOperator::GreaterThan,
        Some(25),
    ));

    let filtering = FilteringOptions::new(vec![filter_expr], columns);

    // Verify the SQL output
    assert_eq!(filtering.to_sql().unwrap(), " WHERE age > 25");
}

#[test]
fn test_and_condition() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("age", ColumnDef::UInt32("age"));
    columns.insert("active", ColumnDef::UInt8("active"));

    // Create a filter for age > 25 AND active = 1
    let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(25));
    let active_condition = FilterCondition::uint8("active", FilterOperator::Equal, Some(1));

    let and_expr = FilterExpression::and(vec![
        FilterExpression::Condition(age_condition),
        FilterExpression::Condition(active_condition),
    ]);

    let filtering = FilteringOptions::new(vec![and_expr], columns);

    // Verify the SQL output
    assert_eq!(
        filtering.to_sql().unwrap(),
        " WHERE (age > 25 AND active = 1)"
    );
}

#[test]
fn test_or_condition() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("age", ColumnDef::UInt32("age"));
    columns.insert("score", ColumnDef::Float64("score"));

    // Create a filter for age > 30 OR score > 90
    let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(30));
    let score_condition =
        FilterCondition::float64("score", FilterOperator::GreaterThan, Some(90.0));

    let or_expr = FilterExpression::or(vec![
        FilterExpression::Condition(age_condition),
        FilterExpression::Condition(score_condition),
    ]);

    let filtering = FilteringOptions::new(vec![or_expr], columns);

    // Verify the SQL output
    assert_eq!(
        filtering.to_sql().unwrap(),
        " WHERE (age > 30 OR score > 90)"
    );
}

#[test]
fn test_complex_condition() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("age", ColumnDef::UInt32("age"));
    columns.insert("score", ColumnDef::Float64("score"));

    // Create a complex filter: (name LIKE '%John%' AND age > 25) OR score > 90
    let name_condition = FilterCondition::string("name", FilterOperator::Like, Some("%John%"));
    let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(25));
    let score_condition =
        FilterCondition::float64("score", FilterOperator::GreaterThan, Some(90.0));

    let name_and_age = FilterExpression::and(vec![
        FilterExpression::Condition(name_condition),
        FilterExpression::Condition(age_condition),
    ]);

    let complex_expr = FilterExpression::or(vec![
        name_and_age,
        FilterExpression::Condition(score_condition),
    ]);

    let filtering = FilteringOptions::new(vec![complex_expr], columns);

    // Verify the SQL output
    assert_eq!(
        filtering.to_sql().unwrap(),
        " WHERE ((lower(name) LIKE lower('%John%') AND age > 25) OR score > 90)"
    );
}

#[test]
fn test_array_filter_has() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("tags", ColumnDef::ArrayString("tags"));

    // Create a filter for tags has 'developer'
    let filter_expr = FilterExpression::Condition(FilterCondition::array_has("tags", "developer"));

    let filtering = FilteringOptions::new(vec![filter_expr], columns);

    // Verify the SQL output - ClickHouse uses the `has` function
    assert_eq!(filtering.to_sql().unwrap(), " WHERE has(tags, 'developer')");
}

#[test]
fn test_array_filter_contains_all() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("tags", ColumnDef::ArrayString("tags"));

    // Create a filter for tags contains all ['developer', 'rust']
    let filter_expr =
        FilterExpression::Condition(FilterCondition::array_contains("tags", "developer,rust"));

    let filtering = FilteringOptions::new(vec![filter_expr], columns);

    // Verify the SQL output - ClickHouse uses the `hasAll` function
    assert_eq!(
        filtering.to_sql().unwrap(),
        " WHERE hasAll(tags, array['developer', 'rust'])"
    );
}

#[test]
fn test_array_filter_with_special_chars() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("tags", ColumnDef::ArrayString("tags"));

    // Create a filter for tags has value with special characters
    let filter_expr =
        FilterExpression::Condition(FilterCondition::array_has("tags", "special'value"));

    let filtering = FilteringOptions::new(vec![filter_expr], columns);

    // Verify the SQL output handles special characters correctly
    assert_eq!(
        filtering.to_sql().unwrap(),
        " WHERE has(tags, 'special''value')"
    );
}

#[test]
fn test_is_null_filter() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("email", ColumnDef::String("email"));

    // Create a filter for email IS NULL
    let filter_expr = FilterExpression::Condition(FilterCondition::string(
        "email",
        FilterOperator::IsNull,
        None,
    ));

    let filtering = FilteringOptions::new(vec![filter_expr], columns);

    // Verify the SQL output
    assert_eq!(filtering.to_sql().unwrap(), " WHERE email IS NULL");
}

#[test]
fn test_multiple_filters() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("age", ColumnDef::UInt32("age"));

    // Create multiple independent filter expressions
    let name_expr = FilterExpression::Condition(FilterCondition::string(
        "name",
        FilterOperator::Like,
        Some("%John%"),
    ));

    let age_expr = FilterExpression::Condition(FilterCondition::uint32(
        "age",
        FilterOperator::GreaterThan,
        Some(25),
    ));

    let filtering = FilteringOptions::new(vec![name_expr, age_expr], columns);

    // Verify the SQL output - should join with AND
    assert_eq!(
        filtering.to_sql().unwrap(),
        " WHERE (lower(name) LIKE lower('%John%') AND age > 25)"
    );
}

#[test]
fn test_deeply_nested_expressions() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("age", ColumnDef::UInt32("age"));
    columns.insert("score", ColumnDef::Float64("score"));
    columns.insert("active", ColumnDef::UInt8("active"));
    columns.insert("status", ColumnDef::String("status"));

    // Create complex nested expression:
    // ((name LIKE '%John%' AND age > 25) OR (score > 90 AND active = 1)) AND status IN ('active', 'pending')

    // Inner group 1: name LIKE '%John%' AND age > 25
    let name_condition = FilterCondition::string("name", FilterOperator::Like, Some("%John%"));
    let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(25));
    let inner_group1 = FilterExpression::and(vec![
        FilterExpression::Condition(name_condition),
        FilterExpression::Condition(age_condition),
    ]);

    // Inner group 2: score > 90 AND active = 1
    let score_condition =
        FilterCondition::float64("score", FilterOperator::GreaterThan, Some(90.0));
    let active_condition = FilterCondition::uint8("active", FilterOperator::Equal, Some(1));
    let inner_group2 = FilterExpression::and(vec![
        FilterExpression::Condition(score_condition),
        FilterExpression::Condition(active_condition),
    ]);

    // Combined with OR: (inner_group1) OR (inner_group2)
    let or_group = FilterExpression::or(vec![inner_group1, inner_group2]);

    // Status IN condition
    let status_condition = FilterCondition::in_values(
        "status",
        FilterOperator::In,
        vec!["active".to_string(), "pending".to_string()],
        Some(ColumnTypeInfo::String),
    );

    // Final AND: (or_group) AND status_condition
    let final_expr = FilterExpression::and(vec![
        or_group,
        FilterExpression::Condition(status_condition),
    ]);

    let filtering = FilteringOptions::new(vec![final_expr], columns);

    // Verify the SQL output contains the nested structure
    let sql = filtering.to_sql().unwrap();
    assert!(sql.contains("((")); // Check for nested parentheses
    assert!(sql.contains("OR"));
    assert!(sql.contains("AND"));
    assert!(sql.contains("IN"));
    assert!(sql.contains("name"));
    assert!(sql.contains("age"));
    assert!(sql.contains("score"));
    assert!(sql.contains("active"));
    assert!(sql.contains("status"));
}

#[test]
fn test_empty_array_value() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("tags", ColumnDef::ArrayString("tags"));

    // Test with empty array value
    let filter_expr = FilterExpression::Condition(FilterCondition::array_has("tags", ""));

    let filtering = FilteringOptions::new(vec![filter_expr], columns);

    // Verify the SQL output correctly handles empty string
    assert_eq!(filtering.to_sql().unwrap(), " WHERE has(tags, '')");
}

#[test]
fn test_null_handling_with_multiple_conditions() {
    // Set up column definitions
    let mut columns = HashMap::new();
    columns.insert("name", ColumnDef::String("name"));
    columns.insert("email", ColumnDef::String("email"));
    columns.insert("age", ColumnDef::UInt32("age"));

    // Create a filter that includes NULL checks: name IS NOT NULL AND (email IS NULL OR age > 25)
    let name_condition = FilterCondition::string("name", FilterOperator::IsNotNull, None);

    let email_condition = FilterCondition::string("email", FilterOperator::IsNull, None);
    let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(25));

    let or_group = FilterExpression::or(vec![
        FilterExpression::Condition(email_condition),
        FilterExpression::Condition(age_condition),
    ]);

    let final_expr =
        FilterExpression::and(vec![FilterExpression::Condition(name_condition), or_group]);

    let filtering = FilteringOptions::new(vec![final_expr], columns);

    // Verify the SQL output
    let sql = filtering.to_sql().unwrap();
    assert!(sql.contains("name IS NOT NULL"));
    assert!(sql.contains("email IS NULL"));
    assert!(sql.contains("age > 25"));
    assert!(sql.contains("AND"));
    assert!(sql.contains("OR"));
}
