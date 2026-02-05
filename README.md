# Dune Analytics MCP Server

A Model Context Protocol (MCP) server that exposes [Dune Analytics API](https://docs.dune.com/api-reference/overview/introduction) endpoints as tools, enabling AI assistants to query blockchain data, manage queries, and control materialized views through natural language.

## Features

- **34 MCP tools** covering all Dune API endpoints
- **Async HTTP client** using httpx for efficient API calls
- **Full type hints** with Pydantic models
- **Comprehensive error handling** with clear messages
- **89% test coverage** with 95 unit tests

## Quick Start (No Installation)

Run directly from GitHub without cloning using `uvx`:

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "dune": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/deacix/dune-mcp", "dune-mcp"],
      "env": {
        "DUNE_API_KEY": "your-api-key"
      }
    }
  }
}
```

### Cursor

Add to `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "dune": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/deacix/dune-mcp", "dune-mcp"],
      "env": {
        "DUNE_API_KEY": "your-api-key"
      }
    }
  }
}
```

That's it! The MCP server will be automatically downloaded and run when your AI assistant starts.

> **Note:** Requires [uv](https://docs.astral.sh/uv/getting-started/installation/) to be installed: `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Installation (Alternative)

If you prefer to install locally:

```bash
# Install from GitHub
pip install git+https://github.com/deacix/dune-mcp

# Or clone and install
git clone https://github.com/deacix/dune-mcp
cd dune-mcp
pip install -e .
```

Set your API key:

```bash
export DUNE_API_KEY="your-api-key"
```

Run the server:

```bash
dune-mcp
```

## MCP Client Configuration (Local Install)

If you installed locally, use simpler config:

### Claude Desktop

```json
{
  "mcpServers": {
    "dune": {
      "command": "dune-mcp",
      "env": {
        "DUNE_API_KEY": "your-api-key"
      }
    }
  }
}
```

### Cursor

```json
{
  "mcpServers": {
    "dune": {
      "command": "dune-mcp",
      "env": {
        "DUNE_API_KEY": "your-api-key"
      }
    }
  }
}
```

## Available Tools

### Executions (9 tools)

| Tool | Description |
|------|-------------|
| `execute_query` | Execute a Dune query by ID |
| `execute_sql` | Execute raw SQL directly on Dune |
| `execute_query_pipeline` | Execute a query with all dependencies |
| `get_execution_result` | Get results of an execution (JSON) |
| `get_execution_result_csv` | Get results as CSV |
| `get_execution_status` | Check execution status |
| `get_latest_result` | Get cached query result (JSON) |
| `get_latest_result_csv` | Get cached result as CSV |
| `cancel_execution` | Cancel a running execution |

### Query Management (9 tools)

| Tool | Description |
|------|-------------|
| `create_query` | Create a new query |
| `read_query` | Read query details and SQL |
| `update_query` | Update an existing query |
| `archive_query` | Archive a query |
| `unarchive_query` | Unarchive a query |
| `private_query` | Make a query private |
| `unprivate_query` | Make a query public |
| `list_queries` | List all queries |
| `get_query_pipeline` | Get query pipeline definition |

### Materialized Views (5 tools)

| Tool | Description |
|------|-------------|
| `get_materialized_view` | Get view details |
| `upsert_materialized_view` | Create or update a view with cron schedule |
| `delete_materialized_view` | Delete a view |
| `list_materialized_views` | List all views |
| `refresh_materialized_view` | Trigger view refresh |

### Tables / Uploads (6 tools)

| Tool | Description |
|------|-------------|
| `create_table` | Create an empty table with schema |
| `upload_csv` | Upload CSV to create table |
| `insert_data` | Insert rows into table |
| `clear_table` | Clear table data |
| `delete_table` | Delete a table |
| `list_tables` | List uploaded tables |

### Datasets (2 tools)

| Tool | Description |
|------|-------------|
| `get_dataset` | Get dataset info and columns |
| `list_datasets` | List available datasets |

### Pipelines (2 tools)

| Tool | Description |
|------|-------------|
| `execute_pipeline` | Execute a pipeline by slug |
| `get_pipeline_status` | Get pipeline execution status |

### Usage (1 tool)

| Tool | Description |
|------|-------------|
| `get_usage` | Get billing usage info (with optional date range) |

## Usage Examples

### Execute a Query

```
"Execute Dune query 1215383 and get the results"
```

The AI will:
1. Call `execute_query(query_id=1215383)`
2. Poll `get_execution_status` until complete
3. Fetch results with `get_execution_result`

### Get Latest Cached Results

```
"Get the latest results from query 6612997, limit to 100 rows"
```

Uses `get_latest_result` for instant cached data without re-execution.

### Create a Materialized View

```
"Create a materialized view named dune.myteam.result_daily_volume from query 6612997 with hourly refresh"
```

Calls `upsert_materialized_view` with `cron_expression="0 */1 * * *"`.

### Execute Raw SQL

```
"Run this SQL on Dune: SELECT * FROM ethereum.transactions LIMIT 10"
```

Uses `execute_sql` for ad-hoc queries without saving.

### Upload Data

```
"Upload this CSV data to a new private table called my_addresses"
```

Uses `upload_csv` with automatic schema inference.

## Development

### Setup

```bash
# Install with dev dependencies
make install-dev

# Or manually
pip install -e ".[dev]"
```

### Commands

```bash
make help        # Show all commands
make test        # Run tests with 80% coverage threshold
make test-cov    # Run tests with HTML coverage report
make lint        # Run ruff linter
make format      # Format code with ruff
make clean       # Remove cache and build files
```

### Project Structure

```
dune/mcp/
├── server.py       # FastMCP server with 34 tools
├── client.py       # Async HTTP client for Dune API
├── models.py       # Pydantic request/response models
├── pyproject.toml  # Package configuration
├── Makefile        # Development commands
├── tests/
│   ├── conftest.py
│   ├── test_server.py
│   ├── test_client.py
│   └── test_models.py
└── README.md
```

### Running Tests

```bash
# Run all tests
make test

# Run specific test file
pytest tests/test_server.py -v

# Run with coverage report
make test-cov
open htmlcov/index.html
```

## API Reference

- [Dune API Documentation](https://docs.dune.com/api-reference/overview/introduction)
- [DuneSQL Reference](https://docs.dune.com/query-engine/writing-efficient-queries)
- [MCP Protocol Specification](https://modelcontextprotocol.io/)

## License

MIT
