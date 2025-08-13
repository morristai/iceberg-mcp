use crate::utils::{CatalogConfig, TableProperties};
use iceberg::spec::SortOrder;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalog;
use iceberg_catalog_rest::RestCatalog;
use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::Parameters},
    model::*,
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, JsonSchema, Deserialize, Serialize)]
struct IcebergObjectRequest {
    namespace: String,
    table: Option<String>,
}

#[derive(Clone)]
pub struct CatalogWrapper {
    catalog: Arc<dyn Catalog>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl CatalogWrapper {
    pub async fn new(config: CatalogConfig) -> Result<Self, McpError> {
        match config {
            CatalogConfig::Rest(config) => {
                let catalog = CatalogWrapper {
                    catalog: Arc::new(RestCatalog::new(config)),
                    tool_router: Self::tool_router(),
                };
                Ok(catalog)
            }
            CatalogConfig::Glue(config) => {
                let catalog = GlueCatalog::new(config).await.map_err(|e| {
                    McpError::internal_error(
                        "fail to create Glue catalog client",
                        Some(json!({"reason": e.to_string()})),
                    )
                })?;
                Ok(CatalogWrapper {
                    catalog: Arc::new(catalog),
                    tool_router: Self::tool_router(),
                })
            }
        }
    }

    #[tool(description = "Get Iceberg namespaces")]
    async fn get_namespaces(&self) -> Result<CallToolResult, McpError> {
        let existing_namespaces = self.catalog.list_namespaces(None).await.map_err(|e| {
            McpError::internal_error(
                "fail to list namespaces",
                Some(json!({"reason": e.to_string()})),
            )
        })?;

        let namespaces: Vec<String> = existing_namespaces
            .iter()
            .map(|ns| ns.to_url_string())
            .collect();

        Ok(CallToolResult::success(vec![Content::json(&namespaces)?]))
    }

    #[tool(description = "Get Iceberg tables")]
    async fn get_tables(
        &self,
        Parameters(IcebergObjectRequest { namespace, .. }): Parameters<IcebergObjectRequest>,
    ) -> Result<CallToolResult, McpError> {
        let namespace = NamespaceIdent::from_vec(vec![namespace]).map_err(|e| {
            McpError::invalid_params(
                "fail to parse namespace",
                Some(json!({"reason": e.to_string()})),
            )
        })?;

        let tables = self.catalog.list_tables(&namespace).await.map_err(|e| {
            McpError::internal_error(
                "fail to list tables",
                Some(json!({"reason": e.to_string()})),
            )
        })?;
        Ok(CallToolResult::success(vec![Content::json(&tables)?]))
    }

    #[tool(description = "Get Iceberg table schema")]
    async fn get_table_schema(
        &self,
        Parameters(IcebergObjectRequest { namespace, table }): Parameters<IcebergObjectRequest>,
    ) -> Result<CallToolResult, McpError> {
        let namespace = NamespaceIdent::from_vec(vec![namespace]).map_err(|e| {
            McpError::invalid_params(
                "fail to parse namespace",
                Some(json!({"reason": e.to_string()})),
            )
        })?;
        let table_ident = TableIdent::new(namespace, table.unwrap());
        let table = self.catalog.load_table(&table_ident).await.map_err(|e| {
            McpError::internal_error("fail to load table", Some(json!({"reason": e.to_string()})))
        })?;
        let schema = table.metadata().current_schema();

        // NOTE: this will display schema `NestedField`
        Ok(CallToolResult::success(vec![Content::json(schema)?]))
    }

    #[tool(description = "Get Iceberg table properties")]
    async fn get_table_properties(
        &self,
        Parameters(IcebergObjectRequest { namespace, table }): Parameters<IcebergObjectRequest>,
    ) -> Result<CallToolResult, McpError> {
        let namespace = NamespaceIdent::from_vec(vec![namespace]).map_err(|e| {
            McpError::invalid_params(
                "fail to parse namespace",
                Some(json!({"reason": e.to_string()})),
            )
        })?;
        let table_ident = TableIdent::new(namespace, table.unwrap());
        let table = self.catalog.load_table(&table_ident).await.map_err(|e| {
            McpError::internal_error("fail to load table", Some(json!({"reason": e.to_string()})))
        })?;

        let metadata = table.metadata();

        let properties = metadata.properties().clone();

        let additional_properties = metadata
            .current_snapshot()
            .map(|snapshot| snapshot.summary().additional_properties.clone())
            .unwrap_or_default();

        let partition_spec = metadata
            .partition_specs_iter()
            .map(|arc| arc.as_ref().clone())
            .collect();

        let sort_order: Vec<SortOrder> = metadata
            .sort_orders_iter()
            .map(|arc| arc.as_ref().clone())
            .collect();

        let table_properties = TableProperties::builder()
            .properties(properties)
            .additional_properties(additional_properties)
            .partition(partition_spec)
            .sort_orders(sort_order)
            .build();

        Ok(CallToolResult::success(vec![Content::json(
            &table_properties,
        )?]))
    }
}

#[tool_handler]
impl ServerHandler for CatalogWrapper {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some("Iceberg MCP Server".to_string()),
            ..Default::default()
        }
    }
}
