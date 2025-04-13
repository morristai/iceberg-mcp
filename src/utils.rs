use anyhow::Result;
use iceberg::spec::{PartitionSpec, SortOrder};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use typed_builder::TypedBuilder;

#[derive(Debug, Serialize, TypedBuilder)]
pub struct TableProperties {
    properties: HashMap<String, String>,
    additional_properties: HashMap<String, String>,
    partition: PartitionSpec,
    sort_orders: Vec<SortOrder>,
}

pub fn init_logging() -> Result<()> {
    let log_level = env::var("LOG_LEVEL")
        .map(|level| match level.to_lowercase().as_str() {
            "error" => Level::ERROR,
            "warn" => Level::WARN,
            "info" => Level::INFO,
            "debug" => Level::DEBUG,
            _ => Level::INFO,
        })
        .unwrap_or(Level::INFO);

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(log_level.into()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    Ok(())
}
