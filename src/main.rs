use crate::server::CatalogWrapper;
use anyhow::Result;
use rmcp::{ServiceExt, transport::stdio};
use std::env;
use utils::*;

mod server;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging()?;

    let catalog_kind =
        env::var("CATALOG_KIND").expect("Environment variable `CATALOG_KIND` not set.");

    let config = match catalog_kind.as_str() {
        "glue" => {
            tracing::info!("Using Glue catalog");
            CatalogConfig::Glue(init_glue_catalog()?)
        }
        "rest" => {
            tracing::info!("Using REST catalog");
            CatalogConfig::Rest(init_rest_catalog()?)
        }
        _ => {
            eprintln!("Invalid catalog kind: {catalog_kind}");
            std::process::exit(1);
        }
    };

    tracing::info!("Starting Iceberg Glue MCP server");

    let service = CatalogWrapper::new(config)
        .await?
        .serve(stdio())
        .await
        .inspect_err(|e| {
            tracing::error!("serving error: {:?}", e);
        })?;

    service.waiting().await?;
    Ok(())
}
