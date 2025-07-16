use anyhow::Result;
use iceberg::spec::{PartitionSpec, SortOrder};
use iceberg_catalog_glue::GlueCatalogConfig;
use iceberg_catalog_hms::{HmsCatalogConfig, HmsThriftTransport};
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
    Hms(HmsCatalogConfig),
}

#[derive(Debug, Serialize, TypedBuilder)]
pub struct TableProperties {
    properties: HashMap<String, String>,
    additional_properties: HashMap<String, String>,
    partition: Vec<PartitionSpec>,
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

pub fn init_hms_catalog() -> Result<HmsCatalogConfig, String> {
    // Required environment variables
    let warehouse = env::var("WAREHOUSE")
        .map_err(|_| "Environment variable `WAREHOUSE` not set.".to_string())?;
    let hms_socket_addr = env::var("HMS_SOCKET_ADDR")
        .map_err(|_| "Environment variable `HMS_SOCKET_ADDR` not set.".to_string())?;

    // Optional environment variables with defaults
    let s3_endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| {
        eprintln!(
            "Environment variable `S3_ENDPOINT` not set. Using default: http://localhost:9000"
        );
        "http://localhost:9000".to_string()
    });
    let s3_access_key_id = env::var("S3_ACCESS_KEY_ID").unwrap_or_else(|_| {
        eprintln!("Environment variable `S3_ACCESS_KEY_ID` not set. Using default: admin");
        "admin".to_string()
    });
    let s3_secret_access_key = env::var("S3_SECRET_ACCESS_KEY").unwrap_or_else(|_| {
        eprintln!("Environment variable `S3_SECRET_ACCESS_KEY` not set. Using default: password");
        "password".to_string()
    });
    let s3_region = env::var("S3_REGION").unwrap_or_else(|_| {
        eprintln!("Environment variable `S3_REGION` not set. Using default: us-east-1");
        "us-east-1".to_string()
    });

    let props: HashMap<String, String> = [
        ("s3.endpoint".to_string(), s3_endpoint),
        ("s3.access.key.id".to_string(), s3_access_key_id),
        ("s3.secret.access.key".to_string(), s3_secret_access_key),
        ("s3.region".to_string(), s3_region),
    ]
    .into_iter()
    .collect();

    let config = HmsCatalogConfig::builder()
        .address(hms_socket_addr)
        .thrift_transport(HmsThriftTransport::Buffered)
        .warehouse(warehouse)
        .props(props)
        .build();

    Ok(config)
}
