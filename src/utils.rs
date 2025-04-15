use anyhow::Result;
use iceberg::spec::{PartitionSpec, SortOrder};
use iceberg_catalog_glue::GlueCatalogConfig;
use iceberg_catalog_rest::RestCatalogConfig;
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use typed_builder::TypedBuilder;

pub enum CatalogConfig {
    Rest(RestCatalogConfig),
    Glue(GlueCatalogConfig),
}

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

pub fn init_rest_catalog() -> Result<RestCatalogConfig> {
    let uri = env::var("REST_URI").unwrap_or_else(|_| {
        eprintln!("Environment variable `REST_URI` not set. Using default URI.");
        "http://localhost:8181".to_string()
    });

    let config = RestCatalogConfig::builder().uri(uri).build();

    Ok(config)
}

pub fn init_glue_catalog() -> Result<GlueCatalogConfig> {
    let profile_name = env::var("PROFILE_NAME").unwrap_or_else(|_| {
        eprintln!("Environment variable `PROFILE_NAME` not set. Using default profile.");
        "default".to_string()
    });
    let props = HashMap::from([("profile_name".to_string(), profile_name)]);

    let warehouse = env::var("WAREHOUSE").expect("Environment variable `WAREHOUSE` not set.");

    let config = GlueCatalogConfig::builder()
        .props(props)
        .warehouse(warehouse)
        .build();

    Ok(config)
}
