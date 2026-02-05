"""
Dune Analytics MCP Server

A FastMCP server that exposes Dune Analytics API endpoints as MCP tools.
Enables AI assistants to query blockchain data, manage queries, and control
materialized views through natural language.

Reference: https://docs.dune.com/api-reference/overview/introduction
"""

import json
from typing import Any

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from client import DuneClient, DuneAPIError

# Load environment variables
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP(
    name="Dune Analytics MCP Server",
    instructions="Query blockchain data via Dune Analytics API",
)

# Initialize client (lazily on first use)
_client: DuneClient | None = None


def get_client() -> DuneClient:
    """Get or create the Dune client."""
    global _client
    if _client is None:
        _client = DuneClient()
    return _client


def format_response(data: Any) -> str:
    """Format response data as JSON string."""
    if isinstance(data, str):
        return data
    return json.dumps(data, indent=2, default=str)


def handle_error(e: Exception) -> str:
    """Handle and format errors."""
    if isinstance(e, DuneAPIError):
        return f"Dune API Error ({e.status_code}): {e.message}"
    return f"Error: {str(e)}"


# =============================================================================
# Execution Tools
# =============================================================================

@mcp.tool()
async def execute_query(
    query_id: int,
    performance: str = "medium",
    query_parameters: str | None = None,
) -> str:
    """
    Execute a Dune query by ID.
    
    Args:
        query_id: The Dune query ID to execute
        performance: Performance tier - "medium" (default) or "large" for complex queries
        query_parameters: Optional JSON string of query parameters {"param1": "value1"}
    
    Returns:
        JSON with execution_id and state. Use get_execution_result to fetch results.
    """
    try:
        client = get_client()
        params = json.loads(query_parameters) if query_parameters else None
        result = await client.execute_query(query_id, performance, params)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def execute_sql(
    query_sql: str,
    performance: str = "medium",
    query_parameters: str | None = None,
) -> str:
    """
    Execute raw SQL directly on Dune without saving a query.
    
    Args:
        query_sql: The DuneSQL query to execute
        performance: Performance tier - "medium" or "large"
        query_parameters: Optional JSON string of parameters
    
    Returns:
        JSON with execution_id and state
    """
    try:
        client = get_client()
        params = json.loads(query_parameters) if query_parameters else None
        result = await client.execute_sql(query_sql, performance, params)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def execute_query_pipeline(
    query_id: int,
    performance: str = "medium",
) -> str:
    """
    Execute a query pipeline that includes all dependent materialized views.
    
    Args:
        query_id: The query ID to execute as a pipeline
        performance: Performance tier
    
    Returns:
        Pipeline execution info
    """
    try:
        client = get_client()
        result = await client.execute_query_pipeline(query_id, performance)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def get_execution_result(
    execution_id: str,
    limit: int | None = None,
    offset: int | None = None,
    filters: str | None = None,
    sort_by: str | None = None,
) -> str:
    """
    Get the results of a query execution.
    
    Args:
        execution_id: The execution ID from execute_query
        limit: Maximum number of rows to return
        offset: Number of rows to skip (for pagination)
        filters: SQL-like WHERE clause for filtering (e.g., "column > 100")
        sort_by: Column to sort results by
    
    Returns:
        JSON with execution results including rows and metadata
    """
    try:
        client = get_client()
        result = await client.get_execution_result(
            execution_id, limit, offset, filters, sort_by
        )
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def get_execution_result_csv(
    execution_id: str,
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """
    Get execution results as CSV format.
    
    Args:
        execution_id: The execution ID
        limit: Maximum number of rows
        offset: Number of rows to skip
    
    Returns:
        CSV formatted string of results
    """
    try:
        client = get_client()
        result = await client.get_execution_result_csv(execution_id, limit, offset)
        return result
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def get_execution_status(execution_id: str) -> str:
    """
    Check the status of a query execution.
    
    Args:
        execution_id: The execution ID to check
    
    Returns:
        JSON with execution status (PENDING, EXECUTING, COMPLETED, FAILED, etc.)
    """
    try:
        client = get_client()
        result = await client.get_execution_status(execution_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def get_latest_result(
    query_id: int,
    limit: int | None = None,
    offset: int | None = None,
    filters: str | None = None,
    sort_by: str | None = None,
) -> str:
    """
    Get the latest cached result of a query without re-executing.
    
    Args:
        query_id: The Dune query ID
        limit: Maximum number of rows
        offset: Number of rows to skip
        filters: SQL-like WHERE clause
        sort_by: Column to sort by
    
    Returns:
        JSON with the latest query results
    """
    try:
        client = get_client()
        result = await client.get_latest_result(
            query_id, limit, offset, filters, sort_by
        )
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def get_latest_result_csv(
    query_id: int,
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """
    Get the latest query result as CSV format.
    
    Args:
        query_id: The Dune query ID
        limit: Maximum number of rows
        offset: Number of rows to skip
    
    Returns:
        CSV formatted string of results
    """
    try:
        client = get_client()
        result = await client.get_latest_result_csv(query_id, limit, offset)
        return result
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def cancel_execution(execution_id: str) -> str:
    """
    Cancel a running query execution.
    
    Args:
        execution_id: The execution ID to cancel
    
    Returns:
        Cancellation confirmation
    """
    try:
        client = get_client()
        result = await client.cancel_execution(execution_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


# =============================================================================
# Query Management Tools
# =============================================================================

@mcp.tool()
async def create_query(
    name: str,
    query_sql: str,
    description: str | None = None,
    is_private: bool = True,
    parameters: str | None = None,
    tags: str | None = None,
) -> str:
    """
    Create a new Dune query.
    
    Args:
        name: Name for the query
        query_sql: The DuneSQL query text
        description: Optional description
        is_private: Whether the query is private (default True)
        parameters: JSON string of parameters [{"key": "param1", "type": "text", "value": "default"}]
        tags: Comma-separated list of tags
    
    Returns:
        JSON with the new query_id
    """
    try:
        client = get_client()
        params_list = json.loads(parameters) if parameters else None
        tags_list = [t.strip() for t in tags.split(",")] if tags else None
        result = await client.create_query(
            name, query_sql, description, is_private, params_list, tags_list
        )
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def read_query(query_id: int) -> str:
    """
    Read a query's details including SQL, parameters, and metadata.
    
    Args:
        query_id: The Dune query ID
    
    Returns:
        JSON with query information
    """
    try:
        client = get_client()
        result = await client.read_query(query_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def update_query(
    query_id: int,
    name: str | None = None,
    query_sql: str | None = None,
    description: str | None = None,
    parameters: str | None = None,
    tags: str | None = None,
) -> str:
    """
    Update an existing query.
    
    Args:
        query_id: The query ID to update
        name: New name (optional)
        query_sql: New SQL (optional)
        description: New description (optional)
        parameters: JSON string of new parameters (optional)
        tags: Comma-separated list of new tags (optional)
    
    Returns:
        Update confirmation
    """
    try:
        client = get_client()
        params_list = json.loads(parameters) if parameters else None
        tags_list = [t.strip() for t in tags.split(",")] if tags else None
        result = await client.update_query(
            query_id, name, query_sql, description, params_list, tags_list
        )
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def archive_query(query_id: int) -> str:
    """
    Archive a query. Archived queries cannot be executed but are not deleted.
    
    Args:
        query_id: The query ID to archive
    
    Returns:
        Archive confirmation
    """
    try:
        client = get_client()
        result = await client.archive_query(query_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def unarchive_query(query_id: int) -> str:
    """
    Unarchive a previously archived query.
    
    Args:
        query_id: The query ID to unarchive
    
    Returns:
        Unarchive confirmation
    """
    try:
        client = get_client()
        result = await client.unarchive_query(query_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def private_query(query_id: int) -> str:
    """
    Make a query private (only visible to owner).
    
    Args:
        query_id: The query ID to make private
    
    Returns:
        Confirmation
    """
    try:
        client = get_client()
        result = await client.private_query(query_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def unprivate_query(query_id: int) -> str:
    """
    Make a query public (visible to everyone).
    
    Args:
        query_id: The query ID to make public
    
    Returns:
        Confirmation
    """
    try:
        client = get_client()
        result = await client.unprivate_query(query_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def list_queries(
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """
    List all queries owned by the API key's account.
    
    Args:
        limit: Maximum number of queries to return
        offset: Number of queries to skip
    
    Returns:
        JSON list of queries with their metadata
    """
    try:
        client = get_client()
        result = await client.list_queries(limit, offset)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def get_query_pipeline(query_id: int) -> str:
    """
    Get the pipeline definition for a query, showing all dependent materialized views.
    
    Args:
        query_id: The query ID
    
    Returns:
        JSON with pipeline definition
    """
    try:
        client = get_client()
        result = await client.get_query_pipeline(query_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


# =============================================================================
# Materialized View Tools
# =============================================================================

@mcp.tool()
async def get_materialized_view(name: str) -> str:
    """
    Get information about a materialized view.
    
    Args:
        name: Full name of the view (e.g., dune.team.result_daily_volume)
    
    Returns:
        JSON with materialized view details
    """
    try:
        client = get_client()
        result = await client.get_materialized_view(name)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def upsert_materialized_view(
    name: str,
    query_id: int,
    cron_expression: str | None = None,
    performance: str = "medium",
) -> str:
    """
    Create or update a materialized view.
    
    Args:
        name: Full name (must start with result_, e.g., dune.team.result_daily_volume)
        query_id: Source query ID
        cron_expression: Refresh schedule (e.g., "0 */1 * * *" for hourly)
        performance: Performance tier - "medium" or "large"
    
    Returns:
        JSON with materialized view info
    """
    try:
        client = get_client()
        result = await client.upsert_materialized_view(
            name, query_id, cron_expression, performance
        )
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def delete_materialized_view(name: str) -> str:
    """
    Delete a materialized view.
    
    Args:
        name: Full name of the view to delete
    
    Returns:
        Deletion confirmation
    """
    try:
        client = get_client()
        result = await client.delete_materialized_view(name)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def list_materialized_views(
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """
    List all materialized views owned by the account.
    
    Args:
        limit: Maximum number to return
        offset: Number to skip
    
    Returns:
        JSON list of materialized views
    """
    try:
        client = get_client()
        result = await client.list_materialized_views(limit, offset)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def refresh_materialized_view(
    name: str,
    performance: str = "medium",
) -> str:
    """
    Trigger a refresh of a materialized view.
    
    Args:
        name: Full name of the view to refresh
        performance: Performance tier
    
    Returns:
        JSON with execution_id for the refresh
    """
    try:
        client = get_client()
        result = await client.refresh_materialized_view(name, performance)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


# =============================================================================
# Table / Upload Tools
# =============================================================================

@mcp.tool()
async def create_table(
    namespace: str,
    table_name: str,
    schema: str,
    description: str | None = None,
    is_private: bool = True,
) -> str:
    """
    Create an empty table for data uploads.
    
    Args:
        namespace: Namespace for the table
        table_name: Name of the table
        schema: JSON string of columns [{"name": "col1", "type": "varchar"}, {"name": "col2", "type": "integer"}]
        description: Optional description
        is_private: Whether the table is private
    
    Returns:
        JSON with table creation result
    """
    try:
        client = get_client()
        schema_list = json.loads(schema)
        result = await client.create_table(
            namespace, table_name, schema_list, description, is_private
        )
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def upload_csv(
    table_name: str,
    csv_data: str,
    description: str | None = None,
    is_private: bool = True,
) -> str:
    """
    Upload CSV data to create a new table with automatic schema inference.
    
    Args:
        table_name: Name for the new table
        csv_data: CSV content as string (with headers)
        description: Optional description
        is_private: Whether the table is private
    
    Returns:
        JSON with upload result
    """
    try:
        client = get_client()
        result = await client.upload_csv(table_name, csv_data, description, is_private)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def insert_data(
    namespace: str,
    table_name: str,
    data: str,
) -> str:
    """
    Insert data into an existing table.
    
    Args:
        namespace: Table namespace
        table_name: Table name
        data: JSON array of row objects [{"col1": "val1", "col2": 123}]
    
    Returns:
        JSON with insert result
    """
    try:
        client = get_client()
        data_list = json.loads(data)
        result = await client.insert_data(namespace, table_name, data_list)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def clear_table(namespace: str, table_name: str) -> str:
    """
    Clear all data from a table while preserving the schema.
    
    Args:
        namespace: Table namespace
        table_name: Table name
    
    Returns:
        Confirmation
    """
    try:
        client = get_client()
        result = await client.clear_table(namespace, table_name)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def delete_table(namespace: str, table_name: str) -> str:
    """
    Permanently delete a table and all its data.
    
    Args:
        namespace: Table namespace
        table_name: Table name
    
    Returns:
        Deletion confirmation
    """
    try:
        client = get_client()
        result = await client.delete_table(namespace, table_name)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def list_tables(
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """
    List all uploaded tables.
    
    Args:
        limit: Maximum number to return
        offset: Number to skip
    
    Returns:
        JSON list of tables
    """
    try:
        client = get_client()
        result = await client.list_tables(limit, offset)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


# =============================================================================
# Dataset Tools
# =============================================================================

@mcp.tool()
async def get_dataset(namespace: str, dataset_name: str) -> str:
    """
    Get information about a dataset including columns and metadata.
    
    Args:
        namespace: Dataset namespace (e.g., "ethereum", "dex")
        dataset_name: Dataset name (e.g., "transactions", "trades")
    
    Returns:
        JSON with dataset information
    """
    try:
        client = get_client()
        result = await client.get_dataset(namespace, dataset_name)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def list_datasets(
    owner: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """
    List available datasets.
    
    Args:
        owner: Filter by owner
        limit: Maximum number to return
        offset: Number to skip
    
    Returns:
        JSON list of datasets
    """
    try:
        client = get_client()
        result = await client.list_datasets(owner, limit, offset)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


# =============================================================================
# Pipeline Tools
# =============================================================================

@mcp.tool()
async def execute_pipeline(
    pipeline_slug: str,
    performance: str = "medium",
) -> str:
    """
    Execute a pipeline by its slug.
    
    Args:
        pipeline_slug: The pipeline slug (e.g., "team/pipeline-name")
        performance: Performance tier - "medium" or "large"
    
    Returns:
        JSON with execution_id for the pipeline run
    """
    try:
        client = get_client()
        result = await client.execute_pipeline(pipeline_slug, performance)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


@mcp.tool()
async def get_pipeline_status(execution_id: str) -> str:
    """
    Get the status of a pipeline execution.
    
    Args:
        execution_id: The pipeline execution ID
    
    Returns:
        JSON with pipeline execution status and progress
    """
    try:
        client = get_client()
        result = await client.get_pipeline_status(execution_id)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


# =============================================================================
# Usage Tools
# =============================================================================

@mcp.tool()
async def get_usage(
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """
    Get billing usage information including credits used, queries, and storage.
    
    Args:
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
    
    Returns:
        JSON with usage information for the billing period
    """
    try:
        client = get_client()
        result = await client.get_usage(start_date, end_date)
        return format_response(result)
    except Exception as e:
        return handle_error(e)


# =============================================================================
# Entry Point
# =============================================================================

def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
