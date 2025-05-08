//! Integration tests for clickhouse-filters
//!
//! These tests use actual ClickHouse instances running in test containers to
//! verify that the generated SQL works correctly in a real database.

use clickhouse::Client;
use eyre::Result;
use std::time::Duration;
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

    // Wait for container to be ready
    println!("Waiting for ClickHouse container to be ready...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("Wait complete, proceeding with setup");

    // Create ClickHouse client using the newer API
    println!("Creating ClickHouse client...");
    let client = Client::default()
        .with_url(format!("http://localhost:{}", http_port))
        .with_database("default")
        .with_user("default")
        .with_password("");

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
