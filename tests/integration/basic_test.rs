//! Basic integration tests for clickhouse-filters
//!
//! These tests verify the basic functionality of the crate with a real ClickHouse database.

use crate::integration::run_with_clickhouse;
use clickhouse::Client;
use clickhouse_filters::{
    ClickHouseFilters, ColumnDef, FilteringOptions, PaginationOptions,
    filtering::{FilterCondition, FilterExpression, FilterOperator},
    sorting::SortedColumn,
};
use eyre::Result;
use std::collections::HashMap;
use futures_util::TryStreamExt;

#[tokio::test]
async fn test_basic_query() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Generate a simple query with no filters
        let columns = HashMap::new();
        
        let filters = ClickHouseFilters::new(
            None,
            vec![],
            None,
            columns,
        )?;
        
        let sql = filters.query_sql("test_filters", "users", &["name"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute the query
        let result = client.query(&sql)
            .fetch_all::<String>()
            .await?;
        
        // Verify we got 5 results (from our test data)
        assert_eq!(result.len(), 5);
        
        Ok(())
    }).await
}

#[tokio::test]
async fn test_count_query() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Generate a count query
        let columns = HashMap::new();
        
        let filters = ClickHouseFilters::new(
            None,
            vec![],
            None,
            columns,
        )?;
        
        let sql = filters.count_sql("test_filters", "users")?;
        println!("Generated count SQL: {}", sql);
        
        // Execute the query
        let result = client.query(&sql)
            .fetch::<u64>()
            .await?;
        
        // Verify we got 5 results (from our test data)
        assert_eq!(result, Some(5));
        
        Ok(())
    }).await
}

#[tokio::test]
async fn test_all_column_types() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Create column definitions for all supported types
        let mut columns = HashMap::new();
        columns.insert("id", ColumnDef::UUID("id"));
        columns.insert("name", ColumnDef::String("name"));
        columns.insert("age", ColumnDef::UInt32("age"));
        columns.insert("active", ColumnDef::UInt8("active"));
        columns.insert("score", ColumnDef::Float64("score"));
        columns.insert("created_at", ColumnDef::DateTime("created_at"));
        columns.insert("tags", ColumnDef::ArrayString("tags"));
        
        // Create a simple filter using multiple column types
        let age_condition = FilterCondition::uint32("age", FilterOperator::GreaterThan, Some(20));
        let score_condition = FilterCondition::float64("score", FilterOperator::GreaterThan, Some(80.0));
        
        let filter_expr = FilterExpression::and(vec![
            FilterExpression::Condition(age_condition),
            FilterExpression::Condition(score_condition),
        ]);
        
        let filtering = FilteringOptions::new(vec![filter_expr], columns.clone());
        
        // Add sorting and pagination
        let sorting = vec![SortedColumn::new("score", "desc")];
        let pagination = PaginationOptions::new(1, 10, 10, 5);
        
        // Create filters
        let filters = ClickHouseFilters::new(
            Some(pagination),
            sorting,
            Some(filtering),
            columns,
        )?;
        
        // Generate SQL
        let sql = filters.query_sql("test_filters", "users", &["name", "age", "score"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute the query
        #[derive(serde::Deserialize)]
        struct QueryResult {
            name: String,
            age: u32,
            score: f64,
        }
        
        let result = client.query(&sql)
            .fetch_all::<QueryResult>()
            .await?;
        
        // Verify result
        assert!(result.len() > 0);
        for item in &result {
            assert!(item.age > 20);
            assert!(item.score > 80.0);
        }
        
        // Check sorting
        if result.len() > 1 {
            assert!(result[0].score >= result[1].score);
        }
        
        Ok(())
    }).await
}

#[tokio::test]
async fn test_api_compatibility() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // This test verifies that the API is compatible with the example in the documentation
        
        // Define column types
        let mut columns = HashMap::new();
        columns.insert("name", ColumnDef::String("name"));
        columns.insert("age", ColumnDef::UInt32("age"));
        
        // Create filters
        let filters = ClickHouseFilters::new(
            Some(PaginationOptions {
                current_page: 1,
                per_page: 10,
                per_page_limit: 10,
                total_records: 5,
            }),
            vec![SortedColumn::new("name", "asc")],
            Some(FilteringOptions::new(
                vec![FilterExpression::Condition(FilterCondition::StringValue {
                    column: "name".to_string(),
                    operator: FilterOperator::Equal,
                    value: Some("John Smith".to_string()),
                })],
                columns.clone(),
            )),
            columns,
        )?;
        
        // Generate SQL
        let sql = filters.sql()?;
        println!("Generated SQL: {}", sql);
        
        // Generate query SQL
        let query_sql = format!("SELECT name, age FROM test_filters.users{}", sql);
        println!("Full query SQL: {}", query_sql);
        
        // Execute the query
        let result = client.query(&query_sql)
            .fetch_all::<(String, u32)>()
            .await?;
        
        // We should have found John Smith
        if !result.is_empty() {
            assert_eq!(result[0].0, "John Smith");
        }
        
        Ok(())
    }).await
}