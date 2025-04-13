use anyhow::Result;
use iceberg_catalog_rest::RestCatalogConfig;
use iceberg_mcp::catalog::rest::RestCatalogWrapper;
use iceberg_mcp::utils::init_logging;
use rmcp::{ServiceExt, transport::stdio};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging()?;

    let uri = env::var("REST_URI").unwrap_or_else(|_| {
        eprintln!("Environment variable `REST_URI` not set. Using default URI.");
        "http://localhost:8181".to_string()
    });

    let config = RestCatalogConfig::builder().uri(uri.to_string()).build();

    tracing::info!("Starting Iceberg Rest MCP server with URI: {}", uri);

    let service = RestCatalogWrapper::new(config)
        .serve(stdio())
        .await
        .inspect_err(|e| {
            tracing::error!("serving error: {:?}", e);
        })?;

    service.waiting().await?;
    Ok(())
}
