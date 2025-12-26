//! PostgreSQL Replication Checker - Rust Edition
//!
//! A Rust implementation of a PostgreSQL logical replication client that connects to a database,
//! creates replication slots, and displays changes in real-time using pg-walstream library.
//!
//! Based on the C++ implementation: https://github.com/fkfk000/replication_checker

mod logging;

use crate::logging::LoggingConfig;
use std::env;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info, warn};

use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
    SharedLsnFeedback,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging from environment variables
    let logging_config = LoggingConfig::from_env()?;
    logging_config.init_logging()?;

    // Check for required environment variables
    let slot_name = env::var("slot_name").unwrap_or_else(|_| "sub".to_string());
    let publication_name = env::var("pub_name").unwrap_or_else(|_| "pub".to_string());

    info!("Slot name: {}", slot_name);
    info!("Publication name: {}", publication_name);

    // Get connection string from environment variable
    let connection_string = env::var("DB_CONNECTION_STRING")
        .map_err(|_| "DB_CONNECTION_STRING environment variable not set")?;

    info!("Using connection string with replication enabled");

    // Create configuration
    let config = ReplicationStreamConfig::new(
        slot_name,
        publication_name,
        2, // Protocol version 2 - supports streaming transactions
        true, // Enable streaming for large transactions
        Duration::from_secs(10), // Feedback interval
        Duration::from_secs(30), // Connection timeout
        Duration::from_secs(60), // Health check interval
        RetryConfig::default(), // Use default retry configuration
    );

    // Run the replication stream
    match run_replication_stream(&connection_string, config).await {
        Ok(()) => {
            info!("Replication stream completed successfully");
            Ok(())
        }
        Err(e) => {
            error!("Replication stream failed: {}", e);
            Err(e)
        }
    }
}

async fn run_replication_stream(
    connection_string: &str,
    config: ReplicationStreamConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Creating logical replication stream");

    // Create the replication stream
    let mut stream = LogicalReplicationStream::new(connection_string, config).await?;

    // Set up LSN feedback for tracking progress
    let lsn_feedback = SharedLsnFeedback::new_shared();
    stream.set_shared_lsn_feedback(lsn_feedback.clone());

    info!("Starting replication stream from latest position");

    // Start replication from the beginning (None = start from latest)
    stream.start(None).await?;

    // Create cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    // Set up graceful shutdown handling
    tokio::spawn(async move {
        signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        warn!("Received interrupt signal, shutting down gracefully...");
        cancel_token_clone.cancel();
    });

    info!("Processing replication events (Press Ctrl+C to stop)...");

    // Process events in a loop
    loop {
        if cancel_token.is_cancelled() {
            info!("Cancellation requested, stopping stream");
            break;
        }

        match stream.next_event(&cancel_token).await? {
            Some(event) => {
                // Display the received event
                info!("Event: {:?}", event);

                // Update LSN feedback after processing
                if let Some(lsn) = event.lsn {
                    lsn_feedback.update_applied_lsn(lsn.value());
                }
            }
            None => {
                // No event available, continue
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    info!("Stopping replication stream");
    stream.stop().await?;
    info!("Graceful shutdown completed");

    Ok(())
}
