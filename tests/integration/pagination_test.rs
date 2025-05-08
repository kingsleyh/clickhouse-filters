//! Integration tests for pagination functionality

use crate::integration::run_with_clickhouse;
use clickhouse_filters::{ClickHouseFilters, ColumnDef, PaginationOptions};
use eyre::Result;
use std::collections::HashMap;
use serde::Deserialize;

#[tokio::test]
async fn test_basic_pagination() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let columns = HashMap::new();
        
        // Create pagination to get first page with 2 records
        let pagination = PaginationOptions::new(1, 2, 10, 5);
        
        // Create filters
        let filters = ClickHouseFilters::new(
            Some(pagination),
            vec![],
            None,
            columns,
        )?;
        
        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute the query
        let result = client.query(&sql)
            .fetch_all::<String>()
            .await?;
        
        // Verify result
        assert_eq!(result.len(), 2);
        
        Ok(())
    }).await
}

#[tokio::test]
async fn test_second_page_pagination() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let columns = HashMap::new();
        
        // Create pagination to get second page with 2 records
        let pagination = PaginationOptions::new(2, 2, 10, 5);
        
        // Create filters
        let filters = ClickHouseFilters::new(
            Some(pagination),
            vec![],
            None,
            columns,
        )?;
        
        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute the query
        let result = client.query(&sql)
            .fetch_all::<String>()
            .await?;
        
        // Verify result
        assert_eq!(result.len(), 2);
        
        // Also test first page to ensure different results
        let first_page = ClickHouseFilters::new(
            Some(PaginationOptions::new(1, 2, 10, 5)),
            vec![],
            None,
            HashMap::new(),
        )?;
        
        let first_page_sql = first_page.query_sql("test_filters", "users", &["name"])?;
        let first_page_result = client.query(&first_page_sql)
            .fetch_all::<String>()
            .await?;
            
        // Ensure second page results are different from first page
        assert_ne!(result, first_page_result);
        
        Ok(())
    }).await
}

#[tokio::test]
async fn test_last_page_pagination() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // First get the total count
        let count: u64 = client.query("SELECT COUNT(*) FROM test_filters.users")
            .fetch_one::<u64>()
            .await?;
        
        let total_records = count;
        
        // Set up column definitions
        let columns = HashMap::new();
        
        // Create pagination to get last page with 2 records per page
        let per_page = 2;
        let last_page = (total_records as f64 / per_page as f64).ceil() as i64;
        
        let pagination = PaginationOptions::new(last_page, per_page, 10, total_records as i64);
        
        // Create filters
        let filters = ClickHouseFilters::new(
            Some(pagination),
            vec![],
            None,
            columns,
        )?;
        
        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute the query
        let result = client.query(&sql)
            .fetch_all::<String>()
            .await?;
        
        // Verify result
        assert!(result.len() > 0);
        assert!(result.len() <= per_page as usize);
        
        Ok(())
    }).await
}

#[tokio::test]
async fn test_pagination_with_counting() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Set up column definitions
        let columns = HashMap::new();
        
        // Create pagination
        let pagination = PaginationOptions::new(1, 3, 10, 0); // 0 total records to be updated
        
        // Create filters
        let mut filters = ClickHouseFilters::new(
            Some(pagination),
            vec![],
            None,
            columns,
        )?;
        
        // Get the count SQL
        let count_sql = filters.count_sql("test_filters", "users")?;
        println!("Count SQL: {}", count_sql);
        
        // Execute count query
        let count: u64 = client.query(&count_sql)
            .fetch_one::<u64>()
            .await?;
        
        let total_records = count;
        
        // Update pagination with correct total records
        filters = ClickHouseFilters::new(
            Some(PaginationOptions::new(1, 3, 10, total_records as i64)),
            vec![],
            None,
            HashMap::new(),
        )?;
        
        // Generate SQL for the query
        let sql = filters.query_sql("test_filters", "users", &["name"])?;
        println!("Generated SQL: {}", sql);
        
        // Execute the query
        let result = client.query(&sql)
            .fetch_all::<String>()
            .await?;
        
        // Verify result
        assert_eq!(result.len(), 3);
        
        Ok(())
    }).await
}