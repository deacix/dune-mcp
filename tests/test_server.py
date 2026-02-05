"""
Unit tests for the MCP server tools.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from dune.dune_mcp import server
from dune.dune_mcp.client import DuneAPIError


@pytest.fixture
def reset_client():
    """Reset the global client before each test."""
    server._client = None
    yield
    server._client = None


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_format_response_dict(self):
        """Test format_response with dict input."""
        data = {"key": "value", "number": 123}
        result = server.format_response(data)
        parsed = json.loads(result)
        assert parsed["key"] == "value"
        assert parsed["number"] == 123

    def test_format_response_string(self):
        """Test format_response with string input."""
        data = "raw string"
        result = server.format_response(data)
        assert result == "raw string"

    def test_handle_error_dune_api_error(self):
        """Test handle_error with DuneAPIError."""
        error = DuneAPIError(400, "Bad Request")
        result = server.handle_error(error)
        assert "400" in result
        assert "Bad Request" in result

    def test_handle_error_generic(self):
        """Test handle_error with generic exception."""
        error = ValueError("Something went wrong")
        result = server.handle_error(error)
        assert "Error:" in result
        assert "Something went wrong" in result


class TestGetClient:
    """Tests for get_client function."""

    def test_get_client_creates_new(self, mock_env, reset_client):
        """Test that get_client creates a new client."""
        client = server.get_client()
        assert client is not None
        assert server._client is client

    def test_get_client_returns_existing(self, mock_env, reset_client):
        """Test that get_client returns existing client."""
        client1 = server.get_client()
        client2 = server.get_client()
        assert client1 is client2


class TestExecutionTools:
    """Tests for execution-related MCP tools."""

    @pytest.mark.asyncio
    async def test_execute_query_success(
        self, mock_env, reset_client, sample_execution_response
    ):
        """Test execute_query tool."""
        with patch.object(
            server, "get_client"
        ) as mock_get_client:
            mock_client = AsyncMock()
            mock_client.execute_query.return_value = sample_execution_response
            mock_get_client.return_value = mock_client

            result = await server.execute_query(1215383, "medium")

            parsed = json.loads(result)
            assert parsed["execution_id"] == "01HKZJ2683PHF9Q9PHHQ8FW4Q1"
            mock_client.execute_query.assert_called_once_with(1215383, "medium", None)

    @pytest.mark.asyncio
    async def test_execute_query_with_params(
        self, mock_env, reset_client, sample_execution_response
    ):
        """Test execute_query with parameters."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.execute_query.return_value = sample_execution_response
            mock_get_client.return_value = mock_client

            params_json = '{"address": "0x123"}'
            result = await server.execute_query(1215383, "large", params_json)

            mock_client.execute_query.assert_called_once_with(
                1215383, "large", {"address": "0x123"}
            )

    @pytest.mark.asyncio
    async def test_execute_query_error(self, mock_env, reset_client):
        """Test execute_query error handling."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.execute_query.side_effect = DuneAPIError(400, "Bad Request")
            mock_get_client.return_value = mock_client

            result = await server.execute_query(1215383)

            assert "400" in result
            assert "Bad Request" in result

    @pytest.mark.asyncio
    async def test_execute_sql_success(
        self, mock_env, reset_client, sample_execution_response
    ):
        """Test execute_sql tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.execute_sql.return_value = sample_execution_response
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM ethereum.transactions LIMIT 10"
            result = await server.execute_sql(sql)

            parsed = json.loads(result)
            assert parsed["execution_id"] == "01HKZJ2683PHF9Q9PHHQ8FW4Q1"

    @pytest.mark.asyncio
    async def test_get_execution_result_success(
        self, mock_env, reset_client, sample_query_result
    ):
        """Test get_execution_result tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_execution_result.return_value = sample_query_result
            mock_get_client.return_value = mock_client

            result = await server.get_execution_result("exec-123", limit=100)

            parsed = json.loads(result)
            assert len(parsed["result"]["rows"]) == 2

    @pytest.mark.asyncio
    async def test_get_execution_result_csv_success(
        self, mock_env, reset_client, sample_csv_result
    ):
        """Test get_execution_result_csv tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_execution_result_csv.return_value = sample_csv_result
            mock_get_client.return_value = mock_client

            result = await server.get_execution_result_csv("exec-123")

            assert "date,volume" in result

    @pytest.mark.asyncio
    async def test_get_execution_status_success(
        self, mock_env, reset_client, sample_execution_status
    ):
        """Test get_execution_status tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_execution_status.return_value = sample_execution_status
            mock_get_client.return_value = mock_client

            result = await server.get_execution_status("exec-123")

            parsed = json.loads(result)
            assert parsed["state"] == "QUERY_STATE_COMPLETED"

    @pytest.mark.asyncio
    async def test_get_latest_result_success(
        self, mock_env, reset_client, sample_query_result
    ):
        """Test get_latest_result tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_latest_result.return_value = sample_query_result
            mock_get_client.return_value = mock_client

            result = await server.get_latest_result(1215383)

            parsed = json.loads(result)
            assert parsed["query_id"] == 1215383

    @pytest.mark.asyncio
    async def test_cancel_execution_success(self, mock_env, reset_client):
        """Test cancel_execution tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.cancel_execution.return_value = {"success": True}
            mock_get_client.return_value = mock_client

            result = await server.cancel_execution("exec-123")

            parsed = json.loads(result)
            assert parsed["success"] is True


class TestQueryManagementTools:
    """Tests for query management MCP tools."""

    @pytest.mark.asyncio
    async def test_create_query_success(self, mock_env, reset_client):
        """Test create_query tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.create_query.return_value = {"query_id": 12345}
            mock_get_client.return_value = mock_client

            result = await server.create_query(
                name="Test Query",
                query_sql="SELECT 1",
                description="Test",
                is_private=True,
                tags="test, example",
            )

            parsed = json.loads(result)
            assert parsed["query_id"] == 12345
            mock_client.create_query.assert_called_once_with(
                "Test Query",
                "SELECT 1",
                "Test",
                True,
                None,
                ["test", "example"],
            )

    @pytest.mark.asyncio
    async def test_read_query_success(
        self, mock_env, reset_client, sample_query_info
    ):
        """Test read_query tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.read_query.return_value = sample_query_info
            mock_get_client.return_value = mock_client

            result = await server.read_query(1215383)

            parsed = json.loads(result)
            assert parsed["name"] == "Test Query"

    @pytest.mark.asyncio
    async def test_update_query_success(self, mock_env, reset_client):
        """Test update_query tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.update_query.return_value = {"success": True}
            mock_get_client.return_value = mock_client

            result = await server.update_query(1215383, name="New Name")

            parsed = json.loads(result)
            assert parsed["success"] is True

    @pytest.mark.asyncio
    async def test_archive_query_success(self, mock_env, reset_client):
        """Test archive_query tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.archive_query.return_value = {"success": True}
            mock_get_client.return_value = mock_client

            result = await server.archive_query(1215383)

            parsed = json.loads(result)
            assert parsed["success"] is True

    @pytest.mark.asyncio
    async def test_list_queries_success(self, mock_env, reset_client):
        """Test list_queries tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_queries.return_value = {"queries": []}
            mock_get_client.return_value = mock_client

            result = await server.list_queries(limit=10)

            parsed = json.loads(result)
            assert "queries" in parsed


class TestMaterializedViewTools:
    """Tests for materialized view MCP tools."""

    @pytest.mark.asyncio
    async def test_get_materialized_view_success(
        self, mock_env, reset_client, sample_materialized_view
    ):
        """Test get_materialized_view tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_materialized_view.return_value = sample_materialized_view
            mock_get_client.return_value = mock_client

            result = await server.get_materialized_view("dune.team.result_daily")

            parsed = json.loads(result)
            assert parsed["query_id"] == 1215383

    @pytest.mark.asyncio
    async def test_upsert_materialized_view_success(
        self, mock_env, reset_client, sample_materialized_view
    ):
        """Test upsert_materialized_view tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.upsert_materialized_view.return_value = sample_materialized_view
            mock_get_client.return_value = mock_client

            result = await server.upsert_materialized_view(
                name="dune.team.result_daily",
                query_id=1215383,
                cron_expression="0 */1 * * *",
            )

            parsed = json.loads(result)
            assert parsed["name"] == "dune.team.result_daily_volume"

    @pytest.mark.asyncio
    async def test_refresh_materialized_view_success(
        self, mock_env, reset_client, sample_execution_response
    ):
        """Test refresh_materialized_view tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.refresh_materialized_view.return_value = (
                sample_execution_response
            )
            mock_get_client.return_value = mock_client

            result = await server.refresh_materialized_view("dune.team.result_daily")

            parsed = json.loads(result)
            assert "execution_id" in parsed

    @pytest.mark.asyncio
    async def test_list_materialized_views_success(self, mock_env, reset_client):
        """Test list_materialized_views tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_materialized_views.return_value = {"views": []}
            mock_get_client.return_value = mock_client

            result = await server.list_materialized_views()

            parsed = json.loads(result)
            assert "views" in parsed


class TestTableTools:
    """Tests for table/upload MCP tools."""

    @pytest.mark.asyncio
    async def test_create_table_success(self, mock_env, reset_client):
        """Test create_table tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.create_table.return_value = {"success": True}
            mock_get_client.return_value = mock_client

            schema = '[{"name": "id", "type": "integer"}]'
            result = await server.create_table("my_ns", "my_table", schema)

            parsed = json.loads(result)
            assert parsed["success"] is True

    @pytest.mark.asyncio
    async def test_upload_csv_success(self, mock_env, reset_client):
        """Test upload_csv tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.upload_csv.return_value = {"success": True}
            mock_get_client.return_value = mock_client

            csv_data = "id,name\n1,test\n2,example"
            result = await server.upload_csv("my_table", csv_data)

            parsed = json.loads(result)
            assert parsed["success"] is True

    @pytest.mark.asyncio
    async def test_insert_data_success(self, mock_env, reset_client):
        """Test insert_data tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.insert_data.return_value = {"success": True}
            mock_get_client.return_value = mock_client

            data = '[{"id": 1, "name": "test"}]'
            result = await server.insert_data("my_ns", "my_table", data)

            parsed = json.loads(result)
            assert parsed["success"] is True

    @pytest.mark.asyncio
    async def test_clear_table_success(self, mock_env, reset_client):
        """Test clear_table tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.clear_table.return_value = {"success": True}
            mock_get_client.return_value = mock_client

            result = await server.clear_table("my_ns", "my_table")

            parsed = json.loads(result)
            assert parsed["success"] is True

    @pytest.mark.asyncio
    async def test_delete_table_success(self, mock_env, reset_client):
        """Test delete_table tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.delete_table.return_value = {"success": True}
            mock_get_client.return_value = mock_client

            result = await server.delete_table("my_ns", "my_table")

            parsed = json.loads(result)
            assert parsed["success"] is True

    @pytest.mark.asyncio
    async def test_list_tables_success(self, mock_env, reset_client):
        """Test list_tables tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_tables.return_value = {"tables": []}
            mock_get_client.return_value = mock_client

            result = await server.list_tables()

            parsed = json.loads(result)
            assert "tables" in parsed


class TestDatasetTools:
    """Tests for dataset MCP tools."""

    @pytest.mark.asyncio
    async def test_get_dataset_success(
        self, mock_env, reset_client, sample_dataset_info
    ):
        """Test get_dataset tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_dataset.return_value = sample_dataset_info
            mock_get_client.return_value = mock_client

            result = await server.get_dataset("ethereum", "transactions")

            parsed = json.loads(result)
            assert parsed["namespace"] == "ethereum"

    @pytest.mark.asyncio
    async def test_list_datasets_success(self, mock_env, reset_client):
        """Test list_datasets tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.list_datasets.return_value = {"datasets": []}
            mock_get_client.return_value = mock_client

            result = await server.list_datasets()

            parsed = json.loads(result)
            assert "datasets" in parsed


class TestPipelineTools:
    """Tests for pipeline MCP tools."""

    @pytest.mark.asyncio
    async def test_execute_pipeline_success(
        self, mock_env, reset_client, sample_execution_response
    ):
        """Test execute_pipeline tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.execute_pipeline.return_value = sample_execution_response
            mock_get_client.return_value = mock_client

            result = await server.execute_pipeline("team/my-pipeline")

            parsed = json.loads(result)
            assert "execution_id" in parsed

    @pytest.mark.asyncio
    async def test_get_pipeline_status_success(
        self, mock_env, reset_client, sample_execution_status
    ):
        """Test get_pipeline_status tool."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_pipeline_status.return_value = sample_execution_status
            mock_get_client.return_value = mock_client

            result = await server.get_pipeline_status("exec-123")

            parsed = json.loads(result)
            assert parsed["state"] == "QUERY_STATE_COMPLETED"


class TestUsageTools:
    """Tests for usage MCP tools."""

    @pytest.mark.asyncio
    async def test_get_usage_success(
        self, mock_env, reset_client, sample_usage_info
    ):
        """Test get_usage tool without date params."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_usage.return_value = sample_usage_info
            mock_get_client.return_value = mock_client

            result = await server.get_usage()

            parsed = json.loads(result)
            assert parsed["credits_used"] == 1500.5
            mock_client.get_usage.assert_called_once_with(None, None)

    @pytest.mark.asyncio
    async def test_get_usage_with_dates_success(
        self, mock_env, reset_client, sample_usage_info
    ):
        """Test get_usage tool with date parameters."""
        with patch.object(server, "get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get_usage.return_value = sample_usage_info
            mock_get_client.return_value = mock_client

            result = await server.get_usage("2024-01-01", "2024-02-01")

            mock_client.get_usage.assert_called_once_with("2024-01-01", "2024-02-01")
