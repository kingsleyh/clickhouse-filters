//! Integration tests for clickhouse-filters
//!
//! These tests use actual ClickHouse instances running in test containers to
//! verify that the generated SQL works correctly in a real database.

use clickhouse::Client;
use eyre::Result;
use testcontainers_modules::{clickhouse::ClickHouse, testcontainers::runners::AsyncRunner};

// Import test modules
pub mod basic_test;
pub mod combined_test;
pub mod filtering_test;
pub mod json_test;
pub mod pagination_test;
pub mod sorting_test;
pub mod test_schema;

/// Run tests with a ClickHouse container
pub async fn run_with_clickhouse<F, Fut>(test: F) -> Result<()>
where
    F: FnOnce(Client) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    println!("==== STARTING CLICKHOUSE TEST SETUP ====");

    // Start ClickHouse container
    println!("Starting ClickHouse container...");
    let start_time = std::time::Instant::now();
    let container = ClickHouse::default().start().await?;
    println!(
        "ClickHouse container started in {}ms",
        start_time.elapsed().as_millis()
    );

    // Get HTTP port for ClickHouse
    println!("Getting HTTP port for ClickHouse...");
    let http_port = container.get_host_port_ipv4(8123).await?;
    println!("ClickHouse HTTP port: {}", http_port);

    // Create ClickHouse client using the newer API
    println!("Creating ClickHouse client...");
    let client = Client::default()
        .with_url(format!("http://localhost:{}", http_port))
        .with_database("default")
        .with_user("default")
        .with_password("");
    
    // Wait for container to be ready with retry logic
    println!("Waiting for ClickHouse container to be ready (with retry)...");
    let max_retries = 10;
    let mut retry_count = 0;
    let mut connected = false;
    
    while retry_count < max_retries && !connected {
        println!("Attempt {} of {}", retry_count + 1, max_retries);
        match client.query("SELECT 1").execute().await {
            Ok(_) => {
                connected = true;
                println!("Successfully connected to ClickHouse!");
            },
            Err(e) => {
                println!("Connection attempt failed: {}", e);
                retry_count += 1;
                // Exponential backoff: wait longer after each failure
                let wait_time = std::time::Duration::from_millis(500 * 2_u64.pow(retry_count as u32));
                println!("Waiting {:?} before next attempt", wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    if !connected {
        return Err(eyre::eyre!("Failed to connect to ClickHouse after {} attempts", max_retries));
    }

    // Set up test schema
    println!("Setting up test schema...");
    test_schema::setup_test_schema(&client).await?;
    println!("Test schema setup complete");

    // Run the test
    println!("Running test...");
    let result = test(client).await;

    // Stop container
    println!("Stopping ClickHouse container...");
    container.stop().await?;
    println!("ClickHouse container stopped");

    result
}
