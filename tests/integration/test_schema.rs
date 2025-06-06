//! Test schema for ClickHouse integration tests

use clickhouse::Client;
use eyre::Result;

/// Helper function for retrying database operations
async fn retry_operation<F, Fut, T>(operation_name: &str, max_retries: usize, f: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut retry_count = 0;
    loop {
        // Add timeout to prevent hanging operations
        match tokio::time::timeout(std::time::Duration::from_secs(5), f()).await {
            Ok(Ok(result)) => {
                println!("{} completed successfully", operation_name);
                return Ok(result);
            }
            Ok(Err(e)) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    return Err(eyre::eyre!(
                        "{} failed after {} attempts: {}",
                        operation_name,
                        max_retries,
                        e
                    ));
                }

                println!("{} attempt {} failed: {}", operation_name, retry_count, e);
                let wait_time = std::time::Duration::from_millis(500);
                println!("Waiting {:?} before retry", wait_time);
                tokio::time::sleep(wait_time).await;
            }
            Err(_) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    return Err(eyre::eyre!(
                        "{} timed out after {} attempts",
                        operation_name,
                        max_retries
                    ));
                }

                println!("{} attempt {} timed out", operation_name, retry_count);
                let wait_time = std::time::Duration::from_millis(500);
                println!("Waiting {:?} before retry", wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
}

/// Set up schema and tables for testing
pub async fn setup_test_schema(client: &Client) -> Result<()> {
    println!("Creating test database if it doesn't exist...");

    // Create database with retry
    retry_operation("Create database", 5, || async {
        client
            .query("CREATE DATABASE IF NOT EXISTS test_filters")
            .execute()
            .await
            .map_err(|e| eyre::eyre!("Database creation failed: {}", e))
    })
    .await?;

    // Create users table with retry
    println!("Creating users table...");
    retry_operation("Create users table", 5, || async {
        client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS test_filters.users (
                    id UUID,
                    name String,
                    email String,
                    age UInt32,
                    active UInt8,
                    score Float64,
                    created_at DateTime,
                    tags Array(String),
                    metadata String,
                    PRIMARY KEY (id)
                ) ENGINE = MergeTree()
                "#,
            )
            .execute()
            .await
            .map_err(|e| eyre::eyre!("Table creation failed: {}", e))
    })
    .await?;

    // Insert sample data with retry
    println!("Inserting sample data...");
    retry_operation("Insert sample data", 5, || async {
        client
            .query(
                r#"
                INSERT INTO test_filters.users
                (id, name, email, age, active, score, created_at, tags, metadata)
                VALUES
                ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'John Smith', 'john@example.com', 25, 1, 94.5, '2022-01-01 10:00:00', ['developer', 'rust'], '{"department": "Engineering", "location": "New York"}'),
                ('6557641d-5cb3-11e7-907b-a6006ad3dba1', 'Jane Doe', 'jane@example.com', 30, 1, 88.2, '2022-01-15 11:30:00', ['manager', 'admin'], '{"department": "Engineering", "location": "San Francisco"}'),
                ('6970c866-5cb3-11e7-907b-a6006ad3dba2', 'Alice Johnson', 'alice@example.com', 22, 1, 91.7, '2022-02-05 09:15:00', ['developer', 'python'], '{"department": "Engineering", "location": "Seattle"}'),
                ('6d89ccaf-5cb3-11e7-907b-a6006ad3dba3', 'Bob Brown', 'bob@example.com', 35, 0, 76.3, '2022-03-10 14:45:00', ['designer', 'ux'], '{"department": "Design", "location": "Los Angeles"}'),
                ('71a2d0f8-5cb3-11e7-907b-a6006ad3dba4', 'Carol White', 'carol@example.com', 28, 1, 82.9, '2022-04-20 16:00:00', ['developer', 'java'], '{"department": "Engineering", "location": "Chicago"}')
                "#,
            )
            .execute()
            .await
            .map_err(|e| eyre::eyre!("Data insertion failed: {}", e))
    }).await?;

    // Create orders table with retry
    println!("Creating orders table...");
    retry_operation("Create orders table", 5, || async {
        client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS test_filters.orders (
                    id UUID,
                    user_id UUID,
                    amount Float64,
                    status String,
                    created_at DateTime,
                    PRIMARY KEY (id)
                ) ENGINE = MergeTree()
                "#,
            )
            .execute()
            .await
            .map_err(|e| eyre::eyre!("Orders table creation failed: {}", e))
    })
    .await?;

    // Insert sample order data with retry
    println!("Inserting sample order data...");
    retry_operation("Insert order data", 5, || async {
        client
            .query(
                r#"
                INSERT INTO test_filters.orders
                (id, user_id, amount, status, created_at)
                VALUES
                ('75bbd341-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 125.99, 'completed', '2022-02-15 10:30:00'),
                ('79d4e78a-5cb3-11e7-907b-a6006ad3dba1', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 89.95, 'processing', '2022-03-20 09:45:00'),
                ('7dedabd3-5cb3-11e7-907b-a6006ad3dba2', '6557641d-5cb3-11e7-907b-a6006ad3dba1', 199.50, 'completed', '2022-02-28 14:15:00'),
                ('81f6bf1c-5cb3-11e7-907b-a6006ad3dba3', '6970c866-5cb3-11e7-907b-a6006ad3dba2', 149.99, 'completed', '2022-04-05 11:20:00'),
                ('860fd365-5cb3-11e7-907b-a6006ad3dba4', '6d89ccaf-5cb3-11e7-907b-a6006ad3dba3', 75.50, 'canceled', '2022-05-10 16:00:00')
                "#,
            )
            .execute()
            .await
            .map_err(|e| eyre::eyre!("Order data insertion failed: {}", e))
    }).await?;

    println!("Sample order data inserted successfully");

    Ok(())
}

/// Clear test database
pub async fn clear_test_schema(client: &Client) -> Result<()> {
    println!("Cleaning up test database...");

    retry_operation("Drop test database", 3, || async {
        client
            .query("DROP DATABASE IF EXISTS test_filters")
            .execute()
            .await
            .map_err(|e| eyre::eyre!("Database drop failed: {}", e))
    })
    .await?;

    println!("Test database cleanup completed successfully");
    Ok(())
}
