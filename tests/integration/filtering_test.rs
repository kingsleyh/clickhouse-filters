//! Integration tests for filtering functionality

use crate::integration::run_with_clickhouse;
use clickhouse_filters::{
    filtering::{FilterCondition, FilterExpression, FilterOperator},
    ClickHouseFilters, ColumnDef, FilteringOptions,
};
use eyre::Result;
use serde::Deserialize;
use std::collections::HashMap;

#[tokio::test]
async fn test_basic_string_filtering() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("name", ColumnDef::String("name"));

        // Create a simple filter with a string equality condition
        let filter_expr = FilterExpression::Condition(FilterCondition::string(
            "name",
            FilterOperator::Equal,
            Some("John Smith"),
        ));

        let filtering = FilteringOptions::new(vec![filter_expr], columns.clone());

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], Some(filtering), columns)?;

        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["id", "name"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        #[derive(Debug, Deserialize, clickhouse::Row)]
        #[allow(dead_code)]
        struct QueryResult {
            id: String, // UUID in the database, but returned as String
            name: String,
        }

        // Instead of trying to get structured data, let's use a simple query to test the filters work
        let count: u64 = client
            .query(&format!("SELECT COUNT(*) FROM ({})", sql))
            .fetch_one::<u64>()
            .await?;

        // Verify result with count
        assert_eq!(count, 1);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_numeric_range_filtering() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("age", ColumnDef::UInt32("age"));

        // Create a filter for age > 25
        let filter_expr = FilterExpression::Condition(FilterCondition::uint32(
            "age",
            FilterOperator::GreaterThan,
            Some(25),
        ));

        let filtering = FilteringOptions::new(vec![filter_expr], columns.clone());

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], Some(filtering), columns)?;

        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "age"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        #[derive(Debug, Deserialize, clickhouse::Row)]
        #[allow(dead_code)]
        struct QueryResult {
            name: String,
            age: u32,
        }

        let result = client.query(&sql).fetch_all::<QueryResult>().await?;

        // Verify result
        assert!(!result.is_empty());
        for item in &result {
            assert!(item.age > 25);
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_array_filtering() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("tags", ColumnDef::ArrayString("tags"));

        // Create a filter for arrays containing 'developer'
        let filter_expr =
            FilterExpression::Condition(FilterCondition::array_has("tags", "developer"));

        let filtering = FilteringOptions::new(vec![filter_expr], columns.clone());

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], Some(filtering), columns)?;

        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "tags"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        #[derive(Debug, Deserialize, clickhouse::Row)]
        #[allow(dead_code)]
        struct QueryResult {
            name: String,
            tags: Vec<String>,
        }

        let result = client.query(&sql).fetch_all::<QueryResult>().await?;

        // Verify result
        assert!(!result.is_empty());
        for item in &result {
            assert!(item.tags.contains(&String::from("developer")));
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_complex_condition_filtering() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("age", ColumnDef::UInt32("age"));
        columns.insert("active", ColumnDef::UInt8("active"));
        columns.insert("score", ColumnDef::Float64("score"));

        // Create a complex filter: (age > 25 AND active = 1) OR score > 90
        let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(25));
        let active_condition = FilterCondition::uint8("active", FilterOperator::Equal, Some(1));
        let score_condition =
            FilterCondition::float64("score", FilterOperator::GreaterThan, Some(90.0));

        let age_and_active = FilterExpression::and(vec![
            FilterExpression::Condition(age_condition),
            FilterExpression::Condition(active_condition),
        ]);

        let complex_expr = FilterExpression::or(vec![
            age_and_active,
            FilterExpression::Condition(score_condition),
        ]);

        let filtering = FilteringOptions::new(vec![complex_expr], columns.clone());

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], Some(filtering), columns)?;

        // Generate SQL for the query
        let sql =
            filters.query_sql("test_filters", "users", &["name", "age", "active", "score"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        #[derive(Debug, Deserialize, clickhouse::Row)]
        #[allow(dead_code)]
        struct QueryResult {
            name: String,
            age: u32,
            active: u8,
            score: f64,
        }

        let result = client.query(&sql).fetch_all::<QueryResult>().await?;

        // Verify result
        assert!(!result.is_empty());
        for item in &result {
            assert!((item.age > 25 && item.active == 1) || item.score > 90.0);
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_date_filtering() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("created_at", ColumnDef::DateTime("created_at"));

        // Create a date range filter
        let filter_expr = FilterExpression::Condition(FilterCondition::date_range(
            "created_at",
            "2022-01-01 00:00:00",
            "2022-03-01 00:00:00",
        ));

        let filtering = FilteringOptions::new(vec![filter_expr], columns.clone());

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], Some(filtering), columns)?;

        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "created_at"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        // Instead of getting structured data, let's just count the results
        let count: u64 = client
            .query(&format!("SELECT COUNT(*) FROM ({})", sql))
            .fetch_one::<u64>()
            .await?;

        // Verify result
        assert!(count > 0);

        Ok(())
    })
    .await
}
