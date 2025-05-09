//! Integration tests for clickhouse-filters
//!
//! These tests use a shared ClickHouse instance running in a test container to
//! verify that the generated SQL works correctly in a real database.

use clickhouse::Client;
use eyre::Result;
use once_cell::sync::Lazy;
use std::time::{Duration, Instant};
use testcontainers_modules::{clickhouse::ClickHouse, testcontainers::runners::AsyncRunner};
use tokio::sync::Mutex;

// Import test modules
pub mod basic_test;
pub mod combined_test;
pub mod filtering_test;
pub mod json_test;
pub mod pagination_test;
pub mod sorting_test;
pub mod test_schema;

// Global shared container info - container and port
static SHARED_CONTAINER: Lazy<Mutex<Option<(u16, bool)>>> = Lazy::new(|| Mutex::new(None));

/// Run tests with a shared ClickHouse container
pub async fn run_with_clickhouse<F, Fut>(test: F) -> Result<()>
where
    F: FnOnce(Client) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    // Get the http port
    let http_port = get_or_create_container().await?;

    // Create a fresh client for each test
    let client = Client::default()
        .with_url(format!("http://localhost:{}", http_port))
        .with_database("default")
        .with_user("default")
        .with_password("");

    // Run the test with a fresh client
    println!("Running test...");
    test(client).await
}

/// Get or create the shared container, returns HTTP port
async fn get_or_create_container() -> Result<u16> {
    let mut container_guard = SHARED_CONTAINER.lock().await;

    // If we already have a container, return the port
    if let Some((http_port, _)) = *container_guard {
        return Ok(http_port);
    }

    println!("==== STARTING CLICKHOUSE TEST SETUP ====");

    // Start ClickHouse container with timeout
    println!("Starting ClickHouse container...");
    let start_time = Instant::now();

    // Use timeout to prevent hanging in CI
    let container =
        tokio::time::timeout(Duration::from_secs(30), ClickHouse::default().start()).await??;

    println!(
        "ClickHouse container started in {}ms",
        start_time.elapsed().as_millis()
    );

    // Get HTTP port for ClickHouse
    println!("Getting HTTP port for ClickHouse...");
    let http_port = container.get_host_port_ipv4(8123).await?;
    println!("ClickHouse HTTP port: {}", http_port);

    // Create ClickHouse client
    println!("Creating ClickHouse client...");
    let client = Client::default()
        .with_url(format!("http://localhost:{}", http_port))
        .with_database("default")
        .with_user("default")
        .with_password("");

    // Wait for container to be ready with retry logic and timeout
    println!("Waiting for ClickHouse container to be ready...");
    let max_retries = 5;
    let mut retry_count = 0;
    let mut connected = false;

    while retry_count < max_retries && !connected {
        println!("Connection attempt {} of {}", retry_count + 1, max_retries);

        // Use timeout for the connection attempt
        match tokio::time::timeout(Duration::from_secs(5), client.query("SELECT 1").execute()).await
        {
            Ok(Ok(_)) => {
                connected = true;
                println!("Successfully connected to ClickHouse!");
            }
            Ok(Err(e)) => {
                println!("Connection attempt failed: {}", e);
                retry_count += 1;

                // Shorter backoff for CI environments
                let wait_time = Duration::from_millis(500);
                println!("Waiting {:?} before next attempt", wait_time);
                tokio::time::sleep(wait_time).await;
            }
            Err(_) => {
                println!("Connection attempt timed out");
                retry_count += 1;

                let wait_time = Duration::from_millis(500);
                println!("Waiting {:?} before next attempt", wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }

    if !connected {
        return Err(eyre::eyre!(
            "Failed to connect to ClickHouse after {} attempts",
            max_retries
        ));
    }

    // Set up test schema once
    println!("Setting up test schema...");
    tokio::time::timeout(
        Duration::from_secs(30),
        test_schema::setup_test_schema(&client),
    )
    .await??;
    println!("Test schema setup complete");

    // Store the container port
    *container_guard = Some((http_port, true));

    // We need to keep this container alive, but we also need to make sure it exists outside
    // of this function. Using a Box::leak to ensure the container stays alive for the
    // entire program lifetime
    Box::leak(Box::new(container));

    Ok(http_port)
}
