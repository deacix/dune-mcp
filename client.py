"""
Dune Analytics API Client

Async HTTP client for interacting with the Dune Analytics API.
Reference: https://docs.dune.com/api-reference/overview/introduction
"""

import json
import os
from typing import Any

import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_URL = "https://api.dune.com/api/v1"


class DuneAPIError(Exception):
    """Exception raised for Dune API errors."""
    
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Dune API Error ({status_code}): {message}")


class DuneClient:
    """
    Async client for the Dune Analytics API.
    
    Provides methods for all Dune API endpoints:
    - Executions: execute queries, get results, check status
    - Queries: create, read, update, archive, list
    - Materialized Views: create, get, delete, list, refresh
    - Tables: create, upload, insert, clear, delete, list
    - Datasets: get, list
    - Usage: get billing usage
    """
    
    def __init__(self, api_key: str | None = None):
        """
        Initialize the Dune client.
        
        Args:
            api_key: Dune API key. If not provided, reads from DUNE_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("DUNE_API_KEY")
        if not self.api_key:
            raise ValueError("DUNE_API_KEY environment variable is required")
        
        self.headers = {
            "X-Dune-API-Key": self.api_key,
            "Content-Type": "application/json",
        }
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        data: str | bytes | None = None,
        content_type: str | None = None,
        timeout: float = 60.0,
    ) -> dict[str, Any] | str:
        """Make an async HTTP request to the Dune API."""
        url = f"{BASE_URL}{endpoint}"
        headers = self.headers.copy()
        
        if content_type:
            headers["Content-Type"] = content_type
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
                content=data,
            )
            
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error", response.text)
                except Exception:
                    error_msg = response.text
                raise DuneAPIError(response.status_code, error_msg)
            
            # Check if response is CSV
            if "text/csv" in response.headers.get("content-type", ""):
                return response.text
            
            try:
                return response.json()
            except Exception:
                return {"raw": response.text}
    
    # =========================================================================
    # Executions
    # =========================================================================
    
    async def execute_query(
        self,
        query_id: int,
        performance: str = "medium",
        query_parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Execute a query by ID.
        
        Args:
            query_id: The query ID to execute
            performance: Performance tier ("medium" or "large")
            query_parameters: Optional query parameters
            
        Returns:
            Dict with execution_id and state
        """
        payload = {"performance": performance}
        if query_parameters:
            payload["query_parameters"] = query_parameters
        
        return await self._request(
            "POST",
            f"/query/{query_id}/execute",
            json_data=payload,
        )
    
    async def execute_sql(
        self,
        query_sql: str,
        performance: str = "medium",
        query_parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Execute raw SQL directly.
        
        Args:
            query_sql: The SQL query to execute
            performance: Performance tier
            query_parameters: Optional query parameters
            
        Returns:
            Dict with execution_id and state
        """
        payload = {
            "query_sql": query_sql,
            "performance": performance,
        }
        if query_parameters:
            payload["query_parameters"] = query_parameters
        
        return await self._request("POST", "/query/execute", json_data=payload)
    
    async def execute_query_pipeline(
        self,
        query_id: int,
        performance: str = "medium",
    ) -> dict[str, Any]:
        """
        Execute a query pipeline.
        
        Args:
            query_id: The query ID
            performance: Performance tier
            
        Returns:
            Pipeline execution info
        """
        return await self._request(
            "POST",
            f"/query/{query_id}/execute/pipeline",
            json_data={"performance": performance},
        )
    
    async def get_execution_result(
        self,
        execution_id: str,
        limit: int | None = None,
        offset: int | None = None,
        filters: str | None = None,
        sort_by: str | None = None,
    ) -> dict[str, Any]:
        """
        Get the results of an execution.
        
        Args:
            execution_id: The execution ID
            limit: Maximum number of rows
            offset: Number of rows to skip
            filters: SQL-like WHERE clause for filtering
            sort_by: Column to sort by
            
        Returns:
            Execution results
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if filters:
            params["filters"] = filters
        if sort_by:
            params["sort_by"] = sort_by
        
        return await self._request(
            "GET",
            f"/execution/{execution_id}/results",
            params=params if params else None,
        )
    
    async def get_execution_result_csv(
        self,
        execution_id: str,
        limit: int | None = None,
        offset: int | None = None,
    ) -> str:
        """
        Get execution results as CSV.
        
        Args:
            execution_id: The execution ID
            limit: Maximum number of rows
            offset: Number of rows to skip
            
        Returns:
            CSV string
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        
        result = await self._request(
            "GET",
            f"/execution/{execution_id}/results/csv",
            params=params if params else None,
        )
        return result if isinstance(result, str) else json.dumps(result)
    
    async def get_execution_status(self, execution_id: str) -> dict[str, Any]:
        """
        Get the status of an execution.
        
        Args:
            execution_id: The execution ID
            
        Returns:
            Execution status info
        """
        return await self._request("GET", f"/execution/{execution_id}/status")
    
    async def get_latest_result(
        self,
        query_id: int,
        limit: int | None = None,
        offset: int | None = None,
        filters: str | None = None,
        sort_by: str | None = None,
    ) -> dict[str, Any]:
        """
        Get the latest result of a query.
        
        Args:
            query_id: The query ID
            limit: Maximum number of rows
            offset: Number of rows to skip
            filters: SQL-like WHERE clause
            sort_by: Column to sort by
            
        Returns:
            Query results
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if filters:
            params["filters"] = filters
        if sort_by:
            params["sort_by"] = sort_by
        
        return await self._request(
            "GET",
            f"/query/{query_id}/results",
            params=params if params else None,
        )
    
    async def get_latest_result_csv(
        self,
        query_id: int,
        limit: int | None = None,
        offset: int | None = None,
    ) -> str:
        """
        Get the latest query result as CSV.
        
        Args:
            query_id: The query ID
            limit: Maximum number of rows
            offset: Number of rows to skip
            
        Returns:
            CSV string
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        
        result = await self._request(
            "GET",
            f"/query/{query_id}/results/csv",
            params=params if params else None,
        )
        return result if isinstance(result, str) else json.dumps(result)
    
    async def cancel_execution(self, execution_id: str) -> dict[str, Any]:
        """
        Cancel an execution.
        
        Args:
            execution_id: The execution ID to cancel
            
        Returns:
            Cancellation result
        """
        return await self._request("POST", f"/execution/{execution_id}/cancel")
    
    # =========================================================================
    # Query Management
    # =========================================================================
    
    async def create_query(
        self,
        name: str,
        query_sql: str,
        description: str | None = None,
        is_private: bool = True,
        parameters: list[dict[str, Any]] | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Create a new query.
        
        Args:
            name: Query name
            query_sql: SQL query text
            description: Optional description
            is_private: Whether the query is private
            parameters: Query parameters
            tags: Query tags
            
        Returns:
            Dict with query_id
        """
        payload = {
            "name": name,
            "query_sql": query_sql,
            "is_private": is_private,
        }
        if description:
            payload["description"] = description
        if parameters:
            payload["parameters"] = parameters
        if tags:
            payload["tags"] = tags
        
        return await self._request("POST", "/query", json_data=payload)
    
    async def read_query(self, query_id: int) -> dict[str, Any]:
        """
        Read a query's details.
        
        Args:
            query_id: The query ID
            
        Returns:
            Query information
        """
        return await self._request("GET", f"/query/{query_id}")
    
    async def update_query(
        self,
        query_id: int,
        name: str | None = None,
        query_sql: str | None = None,
        description: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Update an existing query.
        
        Args:
            query_id: The query ID to update
            name: New name
            query_sql: New SQL
            description: New description
            parameters: New parameters
            tags: New tags
            
        Returns:
            Update result
        """
        payload = {}
        if name is not None:
            payload["name"] = name
        if query_sql is not None:
            payload["query_sql"] = query_sql
        if description is not None:
            payload["description"] = description
        if parameters is not None:
            payload["parameters"] = parameters
        if tags is not None:
            payload["tags"] = tags
        
        return await self._request("PATCH", f"/query/{query_id}", json_data=payload)
    
    async def archive_query(self, query_id: int) -> dict[str, Any]:
        """Archive a query."""
        return await self._request("POST", f"/query/{query_id}/archive")
    
    async def unarchive_query(self, query_id: int) -> dict[str, Any]:
        """Unarchive a query."""
        return await self._request("POST", f"/query/{query_id}/unarchive")
    
    async def private_query(self, query_id: int) -> dict[str, Any]:
        """Make a query private."""
        return await self._request("POST", f"/query/{query_id}/private")
    
    async def unprivate_query(self, query_id: int) -> dict[str, Any]:
        """Make a query public."""
        return await self._request("POST", f"/query/{query_id}/unprivate")
    
    async def list_queries(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """
        List queries owned by the API key's account.
        
        Args:
            limit: Maximum number of queries
            offset: Number to skip
            
        Returns:
            List of queries
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        
        return await self._request(
            "GET",
            "/queries",
            params=params if params else None,
        )
    
    async def get_query_pipeline(self, query_id: int) -> dict[str, Any]:
        """
        Get the pipeline definition for a query.
        
        Args:
            query_id: The query ID
            
        Returns:
            Pipeline definition
        """
        return await self._request("GET", f"/query/{query_id}/pipeline")
    
    # =========================================================================
    # Materialized Views
    # =========================================================================
    
    async def get_materialized_view(self, name: str) -> dict[str, Any]:
        """
        Get a materialized view by name.
        
        Args:
            name: Full name of the materialized view (e.g., dune.team.result_name)
            
        Returns:
            Materialized view info
        """
        return await self._request("GET", f"/materialized-views/{name}")
    
    async def upsert_materialized_view(
        self,
        name: str,
        query_id: int,
        cron_expression: str | None = None,
        performance: str = "medium",
    ) -> dict[str, Any]:
        """
        Create or update a materialized view.
        
        Args:
            name: Full name (must be prefixed with result_)
            query_id: Source query ID
            cron_expression: Refresh schedule
            performance: Performance tier
            
        Returns:
            Materialized view info
        """
        payload = {
            "name": name,
            "query_id": query_id,
            "performance": performance,
        }
        if cron_expression:
            payload["cron_expression"] = cron_expression
        
        return await self._request("POST", "/materialized-views", json_data=payload)
    
    async def delete_materialized_view(self, name: str) -> dict[str, Any]:
        """
        Delete a materialized view.
        
        Args:
            name: Full name of the materialized view
            
        Returns:
            Deletion result
        """
        return await self._request("DELETE", f"/materialized-views/{name}")
    
    async def list_materialized_views(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """
        List all materialized views.
        
        Args:
            limit: Maximum number to return
            offset: Number to skip
            
        Returns:
            List of materialized views
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        
        return await self._request(
            "GET",
            "/materialized-views",
            params=params if params else None,
        )
    
    async def refresh_materialized_view(
        self,
        name: str,
        performance: str = "medium",
    ) -> dict[str, Any]:
        """
        Refresh a materialized view.
        
        Args:
            name: Full name of the materialized view
            performance: Performance tier
            
        Returns:
            Refresh result with execution_id
        """
        return await self._request(
            "POST",
            f"/materialized-views/{name}/refresh",
            json_data={"performance": performance},
        )
    
    # =========================================================================
    # Tables / Uploads
    # =========================================================================
    
    async def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: list[dict[str, str]],
        description: str | None = None,
        is_private: bool = True,
    ) -> dict[str, Any]:
        """
        Create an empty uploaded table.
        
        Args:
            namespace: Namespace for the table
            table_name: Name of the table
            schema: List of column definitions [{"name": "col", "type": "varchar"}]
            description: Optional description
            is_private: Whether the table is private
            
        Returns:
            Table creation result
        """
        payload = {
            "namespace": namespace,
            "table_name": table_name,
            "schema": schema,
            "is_private": is_private,
        }
        if description:
            payload["description"] = description
        
        return await self._request("POST", "/uploads/create", json_data=payload)
    
    async def upload_csv(
        self,
        table_name: str,
        csv_data: str,
        description: str | None = None,
        is_private: bool = True,
    ) -> dict[str, Any]:
        """
        Upload CSV data to create a table.
        
        Args:
            table_name: Name for the new table
            csv_data: CSV content as string
            description: Optional description
            is_private: Whether the table is private
            
        Returns:
            Upload result
        """
        # This endpoint uses multipart form data
        async with httpx.AsyncClient(timeout=120.0) as client:
            files = {"data": ("data.csv", csv_data, "text/csv")}
            data = {
                "table_name": table_name,
                "is_private": str(is_private).lower(),
            }
            if description:
                data["description"] = description
            
            response = await client.post(
                f"{BASE_URL}/uploads/csv",
                headers={"X-Dune-API-Key": self.api_key},
                files=files,
                data=data,
            )
            
            if response.status_code >= 400:
                raise DuneAPIError(response.status_code, response.text)
            
            return response.json()
    
    async def insert_data(
        self,
        namespace: str,
        table_name: str,
        data: list[dict[str, Any]],
        content_type: str = "application/x-ndjson",
    ) -> dict[str, Any]:
        """
        Insert data into an existing table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            data: List of row dictionaries
            content_type: Content type (application/x-ndjson or text/csv)
            
        Returns:
            Insert result
        """
        if content_type == "application/x-ndjson":
            content = "\n".join(json.dumps(row) for row in data)
        else:
            content = json.dumps(data)
        
        return await self._request(
            "POST",
            f"/uploads/{namespace}/{table_name}/insert",
            data=content.encode(),
            content_type=content_type,
        )
    
    async def clear_table(self, namespace: str, table_name: str) -> dict[str, Any]:
        """
        Clear all data from a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            Clear result
        """
        return await self._request(
            "POST",
            f"/uploads/{namespace}/{table_name}/clear",
        )
    
    async def delete_table(self, namespace: str, table_name: str) -> dict[str, Any]:
        """
        Delete a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            Deletion result
        """
        return await self._request(
            "DELETE",
            f"/uploads/{namespace}/{table_name}",
        )
    
    async def list_tables(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """
        List uploaded tables.
        
        Args:
            limit: Maximum number to return
            offset: Number to skip
            
        Returns:
            List of tables
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        
        return await self._request(
            "GET",
            "/uploads",
            params=params if params else None,
        )
    
    # =========================================================================
    # Datasets
    # =========================================================================
    
    async def get_dataset(self, namespace: str, dataset_name: str) -> dict[str, Any]:
        """
        Get dataset information.
        
        Args:
            namespace: Dataset namespace
            dataset_name: Dataset name
            
        Returns:
            Dataset info including columns and metadata
        """
        return await self._request("GET", f"/datasets/{namespace}/{dataset_name}")
    
    async def list_datasets(
        self,
        owner: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """
        List datasets.
        
        Args:
            owner: Filter by owner
            limit: Maximum number to return
            offset: Number to skip
            
        Returns:
            List of datasets
        """
        params = {}
        if owner:
            params["owner"] = owner
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        
        return await self._request(
            "GET",
            "/datasets",
            params=params if params else None,
        )
    
    # =========================================================================
    # Pipelines
    # =========================================================================
    
    async def execute_pipeline(
        self,
        pipeline_slug: str,
        performance: str = "medium",
    ) -> dict[str, Any]:
        """
        Execute a pipeline by slug.
        
        Args:
            pipeline_slug: The pipeline slug (e.g., "team/pipeline-name")
            performance: Performance tier ("medium" or "large")
            
        Returns:
            Pipeline execution info with execution_id
        """
        return await self._request(
            "POST",
            f"/pipelines/{pipeline_slug}/execute",
            json_data={"performance": performance},
        )
    
    async def get_pipeline_status(self, execution_id: str) -> dict[str, Any]:
        """
        Get the status of a pipeline execution.
        
        Args:
            execution_id: The pipeline execution ID
            
        Returns:
            Pipeline execution status
        """
        return await self._request("GET", f"/pipelines/{execution_id}/status")
    
    # =========================================================================
    # Usage
    # =========================================================================
    
    async def get_usage(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        """
        Get billing usage information.
        
        Args:
            start_date: Optional start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format
            
        Returns:
            Usage information including credits, queries, dashboards, storage
        """
        payload = {}
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date
        
        return await self._request(
            "POST",
            "/usage",
            json_data=payload if payload else None,
        )
