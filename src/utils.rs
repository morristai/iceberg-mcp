use anyhow::Result;
use iceberg::CatalogBuilder;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{PartitionSpec, SortOrder};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder,
};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use typed_builder::TypedBuilder;

pub enum CatalogKind {
    Rest(RestCatalog),
    Glue(GlueCatalog),
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

pub async fn init_rest_catalog() -> Result<RestCatalog> {
    let uri = env::var("REST_URI").unwrap_or_else(|_| {
        eprintln!("Environment variable `REST_URI` not set. Using default URI.");
        "http://localhost:8181".to_string()
    });

    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), uri)]),
        )
        .await
        .unwrap();

    Ok(catalog)
}

pub async fn init_glue_catalog() -> Result<GlueCatalog> {
    let mut glue_props = HashMap::new();

    if let Ok(val) = env::var("GLUE_WAREHOUSE") {
        glue_props.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), val);
    }

    if let Ok(profile_name) = env::var("PROFILE_NAME") {
        glue_props.insert("profile_name".to_string(), profile_name);
    } else {
        if let Ok(val) = env::var("AWS_ACCESS_KEY_ID") {
            glue_props.insert(AWS_ACCESS_KEY_ID.to_string(), val);
        }
        if let Ok(val) = env::var("AWS_SECRET_ACCESS_KEY") {
            glue_props.insert(AWS_SECRET_ACCESS_KEY.to_string(), val);
        }

        if let Ok(val) = env::var("GLUE_ENDPOINT") {
            glue_props.insert(GLUE_CATALOG_PROP_URI.to_string(), val);
        }

        glue_props.insert(
            AWS_REGION_NAME.to_string(),
            env::var("AWS_REGION_NAME").unwrap_or_else(|_| "us-east-1".to_string()),
        );

        if let Ok(val) = env::var("S3_ENDPOINT") {
            glue_props.insert(S3_ENDPOINT.to_string(), val);
        }

        if let Ok(val) = env::var("S3_ACCESS_KEY_ID") {
            glue_props.insert(S3_ACCESS_KEY_ID.to_string(), val);
        }

        if let Ok(val) = env::var("S3_SECRET_ACCESS_KEY") {
            glue_props.insert(S3_SECRET_ACCESS_KEY.to_string(), val);
        }

        glue_props.insert(
            S3_REGION.to_string(),
            env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        );
    }

    let catalog = GlueCatalogBuilder::default()
        .load("glue", glue_props)
        .await
        .unwrap();

    Ok(catalog)
}
