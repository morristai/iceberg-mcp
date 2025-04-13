use anyhow::Result;
use iceberg_catalog_glue::GlueCatalogConfig;
use iceberg_mcp::catalog::glue::GlueCatalogWrapper;
use iceberg_mcp::utils::*;
use rmcp::{ServiceExt, transport::stdio};
use std::collections::HashMap;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging()?;

    let profile_name = env::var("PROFILE_NAME").unwrap_or_else(|_| {
        eprintln!("Environment variable `PROFILE_NAME` not set. Using default profile.");
        "default".to_string()
    });

    let props = HashMap::from([("profile_name".to_string(), profile_name)]);

    let warehouse = env::var("WAREHOUSE").expect("Environment variable `WAREHOUSE` not set.");

    let config = GlueCatalogConfig::builder()
        .warehouse(warehouse)
        .props(props)
        .build();

    tracing::info!("Starting Iceberg Glue MCP server");

    let service = GlueCatalogWrapper::new(config)
        .await?
        .serve(stdio())
        .await
        .inspect_err(|e| {
            tracing::error!("serving error: {:?}", e);
        })?;

    service.waiting().await?;
    Ok(())
}
