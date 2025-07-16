# Iceberg MCP

An [MCP](https://modelcontextprotocol.io/introduction) server for [Apache Iceberg](https://iceberg.apache.org/) catalogs with async and logging.

[Iceberg MCP DEMO](https://github.com/user-attachments/assets/13c22d3c-c0a1-4767-acfa-1ffdd1941afd)


### Supported Catalogs

| Catalog Type   | Supported |
|----------------|---------|
| Rest Catalogs  | ✅       |
| AWS Glue       | ✅       |
| Hive Metastore | ✅       |
| S3 Table       | ❌       |

### Supported Tools

| Tools              | Description                                    |
|--------------------|------------------------------------------------|
| `namespaces`       | Get all namespaces in the Iceberg catalog      |
| `tables`           | Get all tables for a given namespace           |
| `table_schema`     | Return the schema for a given table            |
| `table_properties` | Return table properties for a given table      |

## Installation

### Option 1: Download the Release Binary

Download the latest pre-built binary from the [Releases page](https://github.com/morristai/iceberg-mcp/releases).


### Option 2: Build from Source

To build the project manually, ensure you have [Rust](https://www.rust-lang.org/tools/install) installed, then run:

```shell
cargo build --release
```

The compiled binary will be located at: `./target/release/iceberg-mcp`

## Client Configuration

### Claude Desktop

To integrate Iceberg MCP with Claude Desktop:
1. Open `Settings` > `Developer` > `Edit Config`.
2. Update `claude_desktop_config.json` with the appropriate configuration:

- Rest Catalogs

```json
{
  "mcpServers": {
    "iceberg-mcp": {
      "command": "PATH-TO-BINARY/iceberg-mcp",
      "env": {
        "CATALOG_KIND": "rest",
        "REST_URI": "http://localhost:8080",
        "LOG_LEVEL": "info"
      }
    }
  }
}
```

- AWS Glue Catalogs

```json
{
  "mcpServers": {
    "iceberg-mcp": {
      "command": "PATH-TO-BINARY/iceberg-mcp",
      "env": {
        "CATALOG_KIND": "glue",
        "AWS_CONFIG_FILE": "/Users/{your_username}/.aws/config",
        "AWS_SHARED_CREDENTIALS_FILE": "/Users/{your_username}/.aws/credentials",
        "PROFILE_NAME": "default",
        "WAREHOUSE": "s3://{bucket_name}/{namespace}/{table}",
        "LOG_LEVEL": "info"
      }
    }
  }
}
```

## Debugging 

### Claude Desktop

View logs for troubleshooting with:

```shell
tail -n 20 -F ~/Library/Logs/Claude/mcp-server-iceberg-mcp.log
```
