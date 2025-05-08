//! Integration tests for combined functionality
//! 
//! These tests verify that filtering, sorting, and pagination work together correctly.

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
async fn test_filter_sort_paginate() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("age", ColumnDef::UInt32("age"));
        columns.insert("active", ColumnDef::UInt8("active"));
        
        // Create a filter for active users
        let filter_expr = FilterExpression::Condition(FilterCondition::uint8(
            "active", 
            FilterOperator::Equal, 
            Some(1)
        ));
        
        let filtering = FilteringOptions::new(vec![filter_expr], columns.clone());
        
        // Create sorting by age descending
        let sorting = vec![SortedColumn::new("age", "desc")];
        
        // Create pagination for first page with 2 items
        let pagination = PaginationOptions::new(1, 2, 10, 0); // 0 to be updated
        
        // Create filters
        let mut filters = ClickHouseFilters::new(
            Some(pagination),
            sorting,
            Some(filtering),
            columns.clone(),
        )?;
        
        // Get the count SQL first
        let count_sql = filters.count_sql("test_filters", "users")?;
        println!("Count SQL: {}", count_sql);
        
        // Execute count query
        let count_result = client.query(&count_sql)
            .fetch::<u64>()
            .await?;
        
        let total_records = count_result.unwrap_or(0);
        println!("Total active users: {}", total_records);
        
        // Update filters with correct total records
        filters = ClickHouseFilters::new(
            Some(PaginationOptions::new(1, 2, 10, total_records as i64)),
            vec![SortedColumn::new("age", "desc")],
            Some(FilteringOptions::new(vec![FilterExpression::Condition(
                FilterCondition::uint8("active", FilterOperator::Equal, Some(1))
            )], columns.clone())),
            columns,
        )?;
        
        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "age", "active"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute the query
        #[derive(serde::Deserialize, Debug)]
        struct QueryResult {
            name: String,
            age: u32,
            active: u8,
        }
        
        let result = client.query(&sql)
            .fetch_all::<QueryResult>()
            .await?;
        
        // Verify results:
        // 1. Should only have active users (active = 1)
        // 2. Should be sorted by age descending
        // 3. Should have at most 2 results (pagination)
        
        assert!(result.len() <= 2);
        assert!(result.len() > 0);
        
        // Check active status
        for item in &result {
            assert_eq!(item.active, 1);
        }
        
        // Check sorting
        let mut previous_age = u32::MAX;
        for item in &result {
            assert!(item.age <= previous_age);
            previous_age = item.age;
        }
        
        // Test second page
        let second_page = ClickHouseFilters::new(
            Some(PaginationOptions::new(2, 2, 10, total_records as i64)),
            vec![SortedColumn::new("age", "desc")],
            Some(FilteringOptions::new(vec![FilterExpression::Condition(
                FilterCondition::uint8("active", FilterOperator::Equal, Some(1))
            )], columns.clone())),
            columns.clone(),
        )?;
        
        // Only run second page test if we have more than 2 results
        if total_records > 2 {
            let second_page_sql = second_page.query_sql("test_filters", "users", &["name", "age", "active"])?;
            let second_page_result = client.query(&second_page_sql)
                .fetch_all::<QueryResult>()
                .await?;
                
            // Ensure second page has items
            assert!(second_page_result.len() > 0);
            
            // Check active status on second page
            for item in &second_page_result {
                assert_eq!(item.active, 1);
            }
            
            // Second page should have different items than first page
            assert_ne!(result[0].name, second_page_result[0].name);
        }
        
        Ok(())
    }).await
}

#[tokio::test]
async fn test_complex_query() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("name", ColumnDef::String("name"));
        columns.insert("age", ColumnDef::UInt32("age"));
        columns.insert("score", ColumnDef::Float64("score"));
        columns.insert("tags", ColumnDef::ArrayString("tags"));
        
        // Create a complex filter: name LIKE '%o%' AND (age > 25 OR score > 85)
        let name_condition = FilterCondition::string(
            "name", 
            FilterOperator::Like, 
            Some("%o%")
        );
        
        let age_condition = FilterCondition::uint32(
            "age", 
            FilterOperator::GreaterThan, 
            Some(25)
        );
        
        let score_condition = FilterCondition::float64(
            "score", 
            FilterOperator::GreaterThan, 
            Some(85.0)
        );
        
        let age_or_score = FilterExpression::or(vec![
            FilterExpression::Condition(age_condition),
            FilterExpression::Condition(score_condition),
        ]);
        
        let complex_expr = FilterExpression::and(vec![
            FilterExpression::Condition(name_condition),
            age_or_score,
        ]);
        
        let filtering = FilteringOptions::new(vec![complex_expr], columns.clone());
        
        // Create sorting by score descending
        let sorting = vec![SortedColumn::new("score", "desc")];
        
        // Create pagination
        let pagination = PaginationOptions::new(1, 10, 10, 0); // Will be updated
        
        // Create filters
        let mut filters = ClickHouseFilters::new(
            Some(pagination),
            sorting,
            Some(filtering),
            columns.clone(),
        )?;
        
        // Get total count
        let count_sql = filters.count_sql("test_filters", "users")?;
        println!("Count SQL: {}", count_sql);
        
        let count_result = client.query(&count_sql)
            .fetch::<u64>()
            .await?;
        
        let total_records = count_result.unwrap_or(0);
        println!("Total matching records: {}", total_records);
        
        // Update filters with correct count
        let filtering_options = FilteringOptions::new(vec![complex_expr], columns.clone());
        
        filters = ClickHouseFilters::new(
            Some(PaginationOptions::new(1, 10, 10, total_records as i64)),
            vec![SortedColumn::new("score", "desc")],
            Some(filtering_options),
            columns,
        )?;
        
        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name", "age", "score", "tags"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute the query
        #[derive(serde::Deserialize, Debug)]
        struct QueryResult {
            name: String,
            age: u32,
            score: f64,
            tags: Vec<String>,
        }
        
        let result = client.query(&sql)
            .fetch_all::<QueryResult>()
            .await?;
        
        // Verify results
        if !result.is_empty() {
            // Check name contains 'o'
            for item in &result {
                assert!(item.name.contains('o'));
                assert!(item.age > 25 || item.score > 85.0);
            }
            
            // Check sorted by score
            let mut previous_score = f64::MAX;
            for item in &result {
                assert!(item.score <= previous_score);
                previous_score = item.score;
            }
        }
        
        Ok(())
    }).await
}

#[tokio::test]
async fn test_json_filters_with_pagination() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let mut columns = HashMap::new();
        columns.insert("name", ColumnDef::String("name"));
        columns.insert("age", ColumnDef::UInt32("age"));
        columns.insert("active", ColumnDef::UInt8("active"));
        
        // Create JSON filters
        let json_filters = vec![
            clickhouse_filters::filtering::JsonFilter {
                n: "active".to_string(),
                f: "=".to_string(),
                v: "1".to_string(),
                c: Some("AND".to_string()),
            },
            clickhouse_filters::filtering::JsonFilter {
                n: "age".to_string(),
                f: ">".to_string(),
                v: "25".to_string(),
                c: None,
            },
        ];
        
        // Create filtering options from JSON
        let filtering = FilteringOptions::from_json_filters(&json_filters, columns.clone())?;
        
        // Create pagination
        let pagination = PaginationOptions::new(1, 2, 10, 0); // Will be updated
        
        // Create filters
        let mut filters = ClickHouseFilters::new(
            Some(pagination),
            vec![SortedColumn::new("name", "asc")],
            filtering.clone(),
            columns.clone(),
        )?;
        
        // Get count
        let count_sql = filters.count_sql("test_filters", "users")?;
        let count_result = client.query(&count_sql)
            .fetch::<u64>()
            .await?;
        
        let total_records = count_result.unwrap_or(0);
        
        // Update filters with count
        filters = ClickHouseFilters::new(
            Some(PaginationOptions::new(1, 2, 10, total_records as i64)),
            vec![SortedColumn::new("name", "asc")],
            filtering,
            columns,
        )?;
        
        // Generate SQL
        let sql = filters.query_sql("test_filters", "users", &["name", "age", "active"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute query
        #[derive(serde::Deserialize, Debug)]
        struct QueryResult {
            name: String,
            age: u32,
            active: u8,
        }
        
        let result = client.query(&sql)
            .fetch_all::<QueryResult>()
            .await?;
        
        // Verify results
        if !result.is_empty() {
            assert!(result.len() <= 2);
            
            for item in &result {
                assert_eq!(item.active, 1);
                assert!(item.age > 25);
            }
        }
        
        Ok(())
    }).await
}