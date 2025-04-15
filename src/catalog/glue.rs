use crate::utils::TableProperties;
use iceberg::spec::SortOrder;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use rmcp::{Error as McpError, ServerHandler, model::*, tool};
use serde_json::json;
use std::sync::Arc;
use iceberg_catalog_rest::RestCatalog;

#[derive(Debug, Clone)]
pub struct GlueCatalogWrapper(Arc<GlueCatalog>);


#[tool(tool_box)]
impl GlueCatalogWrapper {
    pub async fn new(config: GlueCatalogConfig) -> Result<Self, McpError> {
        let catalog = GlueCatalog::new(config).await.map_err(|e| {
            McpError::internal_error(
                "fail to create Glue catalog client",
                Some(json!({"reason": e.to_string()}
                )),
            )
        })?;

        Ok(Self(Arc::new(catalog)))
    }

    #[tool(description = "Get Iceberg namespaces with Glue catalog")]
    async fn get_namespaces(&self) -> Result<CallToolResult, McpError> {
        let existing_namespaces = self.0.list_namespaces(None).await.map_err(|e| {
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

    #[tool(description = "Get Iceberg tables with Glue catalog")]
    async fn get_tables(
        &self,
        #[tool(param)] namespace: String,
    ) -> Result<CallToolResult, McpError> {
        let namespace = NamespaceIdent::from_vec(vec![namespace]).map_err(|e| {
            McpError::invalid_params(
                "fail to parse namespace",
                Some(json!({"reason": e.to_string()})),
            )
        })?;

        let tables = self.0.list_tables(&namespace).await.map_err(|e| {
            McpError::internal_error(
                "fail to list tables",
                Some(json!({"reason": e.to_string()})),
            )
        })?;
        Ok(CallToolResult::success(vec![Content::json(&tables)?]))
    }

    #[tool(description = "Get Iceberg table schema with Glue catalog")]
    async fn get_table_schema(
        &self,
        #[tool(param)] namespace: String,
        #[tool(param)] table_name: String,
    ) -> Result<CallToolResult, McpError> {
        let namespace = NamespaceIdent::from_vec(vec![namespace]).map_err(|e| {
            McpError::invalid_params(
                "failed to parse namespace",
                Some(json!({ "reason": e.to_string() })),
            )
        })?;

        let table_ident = TableIdent::new(namespace, table_name);
        let table = self.0.load_table(&table_ident).await.map_err(|e| {
            McpError::internal_error(
                "failed to load table",
                Some(json!({ "reason": e.to_string() })),
            )
        })?;

        let schema = table.metadata().current_schema();

        Ok(CallToolResult::success(vec![Content::json(schema)?]))
    }

    #[tool(description = "Get Iceberg table properties with Glue catalog")]
    async fn get_table_properties(
        &self,
        #[tool(param)] namespace: String,
        #[tool(param)] table_name: String,
    ) -> Result<CallToolResult, McpError> {
        let namespace = NamespaceIdent::from_vec(vec![namespace]).map_err(|e| {
            McpError::invalid_params(
                "fail to parse namespace",
                Some(json!({"reason": e.to_string()})),
            )
        })?;
        let table_ident = TableIdent::new(namespace, table_name);
        let table = self.0.load_table(&table_ident).await.map_err(|e| {
            McpError::internal_error("fail to load table", Some(json!({"reason": e.to_string()})))
        })?;

        let metadata = table.metadata();

        let properties = metadata.properties().clone();

        let additional_properties = metadata
            .current_snapshot()
            .map(|snapshot| snapshot.summary().additional_properties.clone())
            .unwrap_or_default();

        let default_partition_spec = table.metadata().default_partition_spec().as_ref().clone();

        let sort_order: Vec<SortOrder> = table
            .metadata()
            .sort_orders_iter()
            .map(|arc| arc.as_ref().clone())
            .collect();

        let table_properties = TableProperties::builder()
            .properties(properties)
            .additional_properties(additional_properties)
            .partition(default_partition_spec)
            .sort_orders(sort_order)
            .build();

        Ok(CallToolResult::success(vec![Content::json(
            &table_properties,
        )?]))
    }
}

#[tool(tool_box)]
impl ServerHandler for GlueCatalogWrapper {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2025_03_26,
            capabilities: ServerCapabilities::builder()
                .enable_prompts()
                .enable_resources()
                .enable_tools()
                .build(),
            server_info: Implementation::from_build_env(),
            instructions: Some("Iceberg Glue Catalog MCP".to_string()),
        }
    }
}
