//! Logging configuration module for PostgreSQL replication checker
//! Supports both console and file logging based on environment variables

use anyhow::{Context, Result};
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::OnceLock;
use tracing::{info, warn};
use tracing_appender::{non_blocking, rolling};
use tracing_subscriber::{
    fmt::{self, time::ChronoUtc},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

// Global guard to keep the non-blocking writer alive
static LOGGING_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

/// Log output destinations
#[derive(Debug, PartialEq)]
pub enum LogOutput {
    /// Log only to console/stderr
    Console,
    /// Log only to file
    File,
    /// Log to both console and file
    All,
}

impl Default for LogOutput {
    fn default() -> Self {
        LogOutput::Console
    }
}

impl FromStr for LogOutput {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "console" => Ok(LogOutput::Console),
            "file" => Ok(LogOutput::File),
            "all" | "both" => Ok(LogOutput::All),
            _ => Err(anyhow::anyhow!("Invalid log output: {}. Valid values are: console, file, all", s)),
        }
    }
}

/// Configuration for logging setup
#[derive(Debug)]
pub struct LoggingConfig {
    /// Log output destination (console, file, or all)
    pub log_output: LogOutput,
    /// Log file directory (default: "./logs")
    pub log_directory: PathBuf,
    /// Log file prefix (default: "replication")
    pub log_file_prefix: String,
    /// Log rotation policy (daily, hourly, never)
    pub rotation: LogRotation,
    /// Log level filter for console (default: info)
    pub log_level: String,
    /// Whether to use JSON format for file logs (default: false)
    pub json_format: bool,
    /// Whether to include ANSI colors in console (default: true)
    pub ansi_enabled: bool,
}

/// Log rotation policies
#[derive(Debug)]
pub enum LogRotation {
    Never,
    Hourly,
    Daily,
    Weekly,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_output: LogOutput::Console,
            log_directory: PathBuf::from("./logs"),
            log_file_prefix: "replication".to_string(),
            rotation: LogRotation::Daily,
            log_level: "info".to_string(),
            json_format: false,
            ansi_enabled: true,
        }
    }
}

impl LoggingConfig {
    /// Create logging configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();

        // Log output configuration - replaces separate console/file flags
        if let Ok(val) = env::var("LOG_OUTPUT") {
            config.log_output = LogOutput::from_str(&val)
                .context("Invalid LOG_OUTPUT value")?;
        }

        // Log directory
        if let Ok(val) = env::var("LOG_DIRECTORY") {
            config.log_directory = PathBuf::from(val);
        }

        // Log file prefix
        if let Ok(val) = env::var("LOG_FILE_PREFIX") {
            config.log_file_prefix = val;
        }

        // Rotation policy
        if let Ok(val) = env::var("LOG_ROTATION") {
            config.rotation = match val.to_lowercase().as_str() {
                "never" | "none" => LogRotation::Never,
                "hourly" | "hour" => LogRotation::Hourly,
                "daily" | "day" => LogRotation::Daily,
                "weekly" | "week" => LogRotation::Weekly,
                _ => {
                    warn!("Invalid LOG_ROTATION value: {}. Using daily rotation.", val);
                    LogRotation::Daily
                }
            };
        }

        // log level
        if let Ok(val) = env::var("LOG_LEVEL") {
            config.log_level = val;
        }

        // JSON format
        if let Ok(val) = env::var("LOG_JSON_FORMAT") {
            config.json_format = val.parse().unwrap_or(false);
        }

        // ANSI colors
        if let Ok(val) = env::var("LOG_ANSI_ENABLED") {
            config.ansi_enabled = val.parse().unwrap_or(true);
        }

        Ok(config)
    }

    /// Initialize logging based on the configuration
    pub fn init_logging(self) -> Result<()> {
        let mut layers = Vec::new();

        // Determine which outputs to enable based on the enum
        let console_enabled = matches!(self.log_output, LogOutput::Console | LogOutput::All);
        let file_enabled = matches!(self.log_output, LogOutput::File | LogOutput::All);

        // Console layer
        if console_enabled {
            let console_filter = EnvFilter::try_new(&self.log_level)
                .context("Invalid console log level")?;

            let console_layer = fmt::layer()
                .with_writer(io::stderr)
                .with_timer(ChronoUtc::rfc_3339())
                .with_ansi(self.ansi_enabled)
                .with_target(true)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_filter(console_filter);

            layers.push(console_layer.boxed());
        }

        // File layer
        if file_enabled {
            // Create log directory if it doesn't exist
            if !self.log_directory.exists() {
                fs::create_dir_all(&self.log_directory)
                    .context("Failed to create log directory")?;
            }

            let file_filter = EnvFilter::try_new(&self.log_level)
                .context("Invalid file log level")?;

            let file_writer = self.create_file_writer()?;
            let (non_blocking_writer, guard) = non_blocking(file_writer);

            // Store the guard globally to keep it alive for the entire application lifecycle
            if let Err(_) = LOGGING_GUARD.set(guard) {
                warn!("Logging guard already set, this may cause log loss");
            }

            let file_layer = if self.json_format {
                fmt::layer()
                    .json()
                    .with_writer(non_blocking_writer)
                    .with_timer(ChronoUtc::rfc_3339())
                    .with_ansi(false)
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_filter(file_filter)
                    .boxed()
            } else {
                fmt::layer()
                    .with_writer(non_blocking_writer)
                    .with_timer(ChronoUtc::rfc_3339())
                    .with_ansi(false)
                    .with_target(true)
                    .with_thread_ids(false)
                    .with_thread_names(false)
                    .with_filter(file_filter)
                    .boxed()
            };

            layers.push(file_layer);
        }

        if layers.is_empty() {
            return Err(anyhow::anyhow!("No logging layers enabled"));
        }

        // Initialize the subscriber
        tracing_subscriber::registry()
            .with(layers)
            .try_init()
            .context("Failed to initialize logging")?;

        info!(
            "Logging initialized: output={:?}, json={}",
            self.log_output, self.json_format
        );

        Ok(())
    }

    /// Create file writer based on rotation policy
    fn create_file_writer(&self) -> Result<Box<dyn io::Write + Send + Sync>> {
        match self.rotation {
            LogRotation::Never => {
                let log_file = self.log_directory.join(format!("{}.log", self.log_file_prefix));
                let file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_file)
                    .context("Failed to open log file")?;
                Ok(Box::new(file))
            }
            LogRotation::Hourly => {
                let appender = rolling::hourly(&self.log_directory, &self.log_file_prefix);
                Ok(Box::new(appender))
            }
            LogRotation::Daily => {
                let appender = rolling::daily(&self.log_directory, &self.log_file_prefix);
                Ok(Box::new(appender))
            }
            LogRotation::Weekly => {
                // tracing-appender doesn't have weekly, so we use daily
                warn!("Weekly rotation not supported, using daily rotation");
                let appender = rolling::daily(&self.log_directory, &self.log_file_prefix);
                Ok(Box::new(appender))
            }
        }
    }
}
