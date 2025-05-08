//! Integration tests for sorting

use crate::integration::run_with_clickhouse;
use clickhouse::Client;
use clickhouse_filters::sorting::{SortedColumn, Sorting};
use eyre::Result;

/// Run an actual query with the generated SQL
async fn run_query(client: &Client, sorting: &Sorting) -> Result<Vec<String>> {
    // Build the query using the generated SQL
    let query = format!("SELECT name FROM test_filters.users{} LIMIT 10", sorting.sql);
    println!("Executing query: {}", query);
    
    // Execute the query and collect results
    let result = client
        .query(&query)
        .fetch_all()
        .await?
        .rows::<String>()?
        .collect::<Vec<_>>();
    
    Ok(result)
}

#[tokio::test]
async fn test_sorting_asc() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Create ascending sort
        let sorting = Sorting::new(vec![SortedColumn::new("name", "asc")]);
        
        // Run query with the generated SQL
        let results = run_query(&client, &sorting).await?;
        
        // Verify results are in ascending order
        let mut expected = results.clone();
        expected.sort();
        
        assert_eq!(results, expected);
        assert_eq!(results[0], "Alice Johnson"); // First alphabetically
        
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_sorting_desc() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Create descending sort
        let sorting = Sorting::new(vec![SortedColumn::new("name", "desc")]);
        
        // Run query with the generated SQL
        let results = run_query(&client, &sorting).await?;
        
        // Verify results are in descending order
        let mut expected = results.clone();
        expected.sort();
        expected.reverse();
        
        assert_eq!(results, expected);
        assert_eq!(results[0], "John Smith"); // Last alphabetically
        
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_sorting_multiple_columns() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Create multi-column sort (age desc, name asc)
        let sorting = Sorting::new(vec![
            SortedColumn::new("age", "desc"),
            SortedColumn::new("name", "asc"),
        ]);
        
        // Run a query that returns both columns
        let query = format!("SELECT name, age FROM test_filters.users{} LIMIT 10", sorting.sql);
        println!("Executing query: {}", query);
        
        let results = client
            .query(&query)
            .fetch_all()
            .await?
            .rows::<(String, u32)>()?
            .collect::<Vec<_>>();
        
        // Verify the first result is the oldest person
        assert_eq!(results[0].0, "Bob Brown");
        assert_eq!(results[0].1, 35);
        
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_empty_sorting() -> Result<()> {
    run_with_clickhouse(|client| async move {
        // Create empty sort
        let sorting = Sorting::new(vec![]);
        
        // Run query with the generated SQL (should have no ORDER BY)
        let results = run_query(&client, &sorting).await?;
        
        // Just verify we got results back
        assert!(!results.is_empty());
        assert_eq!(results.len(), 5);
        
        Ok(())
    })
    .await
}