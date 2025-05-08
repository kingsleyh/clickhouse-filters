//! Integration tests for JSON filtering functionality
//!
//! These tests verify that JSON-based filters work correctly with ClickHouse.

use crate::integration::run_with_clickhouse;
use clickhouse_filters::{filtering::JsonFilter, ClickHouseFilters, ColumnDef, FilteringOptions};
use eyre::Result;
use serde::Deserialize;
use std::collections::HashMap;

#[tokio::test]
async fn test_basic_json_filter() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("name", ColumnDef::String("name"));
        columns.insert("age", ColumnDef::UInt32("age"));

        // Create a simple JSON filter for age > 25
        let json_filters = vec![JsonFilter {
            n: "age".to_string(),
            f: ">".to_string(),
            v: "25".to_string(),
            c: None,
        }];

        // Create filtering options from JSON
        let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone())?;

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], filtering, columns)?;

        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "age"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        #[derive(Debug, Deserialize, clickhouse::Row)]
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
async fn test_multiple_json_filters() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("name", ColumnDef::String("name"));
        columns.insert("age", ColumnDef::UInt32("age"));
        columns.insert("active", ColumnDef::UInt8("active"));

        // Create multiple JSON filters with AND connector
        let json_filters = vec![
            JsonFilter {
                n: "age".to_string(),
                f: ">".to_string(),
                v: "25".to_string(),
                c: Some("AND".to_string()),
            },
            JsonFilter {
                n: "active".to_string(),
                f: "=".to_string(),
                v: "1".to_string(),
                c: None,
            },
        ];

        // Create filtering options from JSON
        let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone())?;

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], filtering, columns)?;

        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "age", "active"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        #[derive(Debug, Deserialize, clickhouse::Row)]
        struct QueryResult {
            name: String,
            age: u32,
            active: u8,
        }

        let result = client.query(&sql).fetch_all::<QueryResult>().await?;

        // Verify result
        assert!(!result.is_empty());
        for item in &result {
            assert!(item.age > 25);
            assert_eq!(item.active, 1);
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_json_filters_with_or() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("name", ColumnDef::String("name"));
        columns.insert("age", ColumnDef::UInt32("age"));
        columns.insert("active", ColumnDef::UInt8("active"));

        // Create multiple JSON filters with OR connector
        let json_filters = vec![
            JsonFilter {
                n: "age".to_string(),
                f: "<".to_string(),
                v: "25".to_string(),
                c: Some("OR".to_string()),
            },
            JsonFilter {
                n: "active".to_string(),
                f: "=".to_string(),
                v: "0".to_string(),
                c: None,
            },
        ];

        // Create filtering options from JSON
        let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone())?;

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], filtering, columns)?;

        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "age", "active"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        #[derive(Debug, Deserialize, clickhouse::Row)]
        struct QueryResult {
            name: String,
            age: u32,
            active: u8,
        }

        let result = client.query(&sql).fetch_all::<QueryResult>().await?;

        // Verify result
        assert!(!result.is_empty());
        for item in &result {
            assert!(item.age < 25 || item.active == 0);
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_json_filters_with_array() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("name", ColumnDef::String("name"));
        columns.insert("tags", ColumnDef::ArrayString("tags"));

        // Create JSON filter for array contains
        let json_filters = vec![JsonFilter {
            n: "tags".to_string(),
            f: "ARRAY HAS".to_string(),
            v: "developer".to_string(),
            c: None,
        }];

        // Create filtering options from JSON
        let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone())?;

        // Create filters
        let filters = ClickHouseFilters::new(None, vec![], filtering, columns)?;

        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "tags"])?;
        println!("Generated SQL: {}", sql);

        // Execute the query
        #[derive(Debug, Deserialize, clickhouse::Row)]
        struct QueryResult {
            name: String,
            tags: Vec<String>,
        }

        let result = client.query(&sql).fetch_all::<QueryResult>().await?;

        // Verify result
        assert!(!result.is_empty());
        for item in &result {
            assert!(item.tags.contains(&"developer".to_string()));
        }

        Ok(())
    })
    .await
}
