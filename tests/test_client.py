"""
Unit tests for the DuneClient class.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dune.dune_mcp.client import BASE_URL, DuneAPIError, DuneClient


class TestDuneClientInit:
    """Tests for DuneClient initialization."""

    def test_init_with_api_key(self):
        """Test initialization with explicit API key."""
        client = DuneClient(api_key="test-key")
        assert client.api_key == "test-key"
        assert "X-Dune-API-Key" in client.headers
        assert client.headers["X-Dune-API-Key"] == "test-key"

    def test_init_from_env(self, mock_env, mock_api_key):
        """Test initialization from environment variable."""
        client = DuneClient()
        assert client.api_key == mock_api_key

    def test_init_missing_key_raises(self):
        """Test that missing API key raises ValueError."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="DUNE_API_KEY"):
                DuneClient()


class TestDuneClientRequest:
    """Tests for the _request method."""

    @pytest.mark.asyncio
    async def test_request_success(self, mock_env):
        """Test successful API request."""
        client = DuneClient()
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {"content-type": "application/json"}
        
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.request.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            result = await client._request("GET", "/test")
            
            assert result == {"data": "test"}
            mock_client.request.assert_called_once()

    @pytest.mark.asyncio
    async def test_request_csv_response(self, mock_env):
        """Test handling CSV response."""
        client = DuneClient()
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "col1,col2\nval1,val2"
        mock_response.headers = {"content-type": "text/csv"}
        
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.request.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            result = await client._request("GET", "/test")
            
            assert result == "col1,col2\nval1,val2"

    @pytest.mark.asyncio
    async def test_request_error_400(self, mock_env):
        """Test handling 400 error."""
        client = DuneClient()
        
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "Bad Request"}
        mock_response.text = '{"error": "Bad Request"}'
        
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.request.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            with pytest.raises(DuneAPIError) as exc_info:
                await client._request("GET", "/test")
            
            assert exc_info.value.status_code == 400
            assert "Bad Request" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_request_error_401(self, mock_env):
        """Test handling 401 unauthorized error."""
        client = DuneClient()
        
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"error": "Invalid API Key"}
        mock_response.text = '{"error": "Invalid API Key"}'
        
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.request.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            with pytest.raises(DuneAPIError) as exc_info:
                await client._request("GET", "/test")
            
            assert exc_info.value.status_code == 401


class TestDuneClientExecutions:
    """Tests for execution-related methods."""

    @pytest.mark.asyncio
    async def test_execute_query(self, mock_env, sample_execution_response):
        """Test execute_query method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_execution_response
            
            result = await client.execute_query(1215383, "medium")
            
            mock_request.assert_called_once_with(
                "POST",
                "/query/1215383/execute",
                json_data={"performance": "medium"},
            )
            assert result["execution_id"] == "01HKZJ2683PHF9Q9PHHQ8FW4Q1"

    @pytest.mark.asyncio
    async def test_execute_query_with_params(self, mock_env, sample_execution_response):
        """Test execute_query with parameters."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_execution_response
            
            params = {"address": "0x123", "limit": 100}
            result = await client.execute_query(1215383, "large", params)
            
            mock_request.assert_called_once_with(
                "POST",
                "/query/1215383/execute",
                json_data={"performance": "large", "query_parameters": params},
            )

    @pytest.mark.asyncio
    async def test_execute_sql(self, mock_env, sample_execution_response):
        """Test execute_sql method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_execution_response
            
            sql = "SELECT * FROM ethereum.transactions LIMIT 10"
            result = await client.execute_sql(sql)
            
            mock_request.assert_called_once_with(
                "POST",
                "/query/execute",
                json_data={"query_sql": sql, "performance": "medium"},
            )

    @pytest.mark.asyncio
    async def test_get_execution_result(self, mock_env, sample_query_result):
        """Test get_execution_result method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_query_result
            
            result = await client.get_execution_result("exec-123", limit=100)
            
            mock_request.assert_called_once_with(
                "GET",
                "/execution/exec-123/results",
                params={"limit": 100},
            )
            assert result["result"]["rows"][0]["volume"] == 1000000

    @pytest.mark.asyncio
    async def test_get_execution_result_csv(self, mock_env, sample_csv_result):
        """Test get_execution_result_csv method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_csv_result
            
            result = await client.get_execution_result_csv("exec-123")
            
            mock_request.assert_called_once_with(
                "GET",
                "/execution/exec-123/results/csv",
                params=None,
            )
            assert "date,volume" in result

    @pytest.mark.asyncio
    async def test_get_execution_status(self, mock_env, sample_execution_status):
        """Test get_execution_status method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_execution_status
            
            result = await client.get_execution_status("exec-123")
            
            mock_request.assert_called_once_with("GET", "/execution/exec-123/status")
            assert result["state"] == "QUERY_STATE_COMPLETED"

    @pytest.mark.asyncio
    async def test_get_latest_result(self, mock_env, sample_query_result):
        """Test get_latest_result method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_query_result
            
            result = await client.get_latest_result(1215383, limit=50, offset=10)
            
            mock_request.assert_called_once_with(
                "GET",
                "/query/1215383/results",
                params={"limit": 50, "offset": 10},
            )

    @pytest.mark.asyncio
    async def test_cancel_execution(self, mock_env):
        """Test cancel_execution method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            result = await client.cancel_execution("exec-123")
            
            mock_request.assert_called_once_with("POST", "/execution/exec-123/cancel")


class TestDuneClientQueries:
    """Tests for query management methods."""

    @pytest.mark.asyncio
    async def test_create_query(self, mock_env):
        """Test create_query method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"query_id": 12345}
            
            result = await client.create_query(
                name="Test Query",
                query_sql="SELECT 1",
                description="A test",
                is_private=True,
                tags=["test"],
            )
            
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            assert call_args[0][0] == "POST"
            assert call_args[0][1] == "/query"
            assert call_args[1]["json_data"]["name"] == "Test Query"
            assert result["query_id"] == 12345

    @pytest.mark.asyncio
    async def test_read_query(self, mock_env, sample_query_info):
        """Test read_query method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_query_info
            
            result = await client.read_query(1215383)
            
            mock_request.assert_called_once_with("GET", "/query/1215383")
            assert result["name"] == "Test Query"

    @pytest.mark.asyncio
    async def test_update_query(self, mock_env):
        """Test update_query method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            result = await client.update_query(
                1215383,
                name="Updated Query",
                query_sql="SELECT 2",
            )
            
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            assert call_args[0][0] == "PATCH"
            assert call_args[0][1] == "/query/1215383"

    @pytest.mark.asyncio
    async def test_archive_query(self, mock_env):
        """Test archive_query method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            await client.archive_query(1215383)
            
            mock_request.assert_called_once_with("POST", "/query/1215383/archive")

    @pytest.mark.asyncio
    async def test_unarchive_query(self, mock_env):
        """Test unarchive_query method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            await client.unarchive_query(1215383)
            
            mock_request.assert_called_once_with("POST", "/query/1215383/unarchive")

    @pytest.mark.asyncio
    async def test_list_queries(self, mock_env):
        """Test list_queries method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"queries": []}
            
            await client.list_queries(limit=10, offset=5)
            
            mock_request.assert_called_once_with(
                "GET",
                "/queries",
                params={"limit": 10, "offset": 5},
            )


class TestDuneClientMaterializedViews:
    """Tests for materialized view methods."""

    @pytest.mark.asyncio
    async def test_get_materialized_view(self, mock_env, sample_materialized_view):
        """Test get_materialized_view method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_materialized_view
            
            result = await client.get_materialized_view("dune.team.result_daily")
            
            mock_request.assert_called_once_with(
                "GET",
                "/materialized-views/dune.team.result_daily",
            )
            assert result["query_id"] == 1215383

    @pytest.mark.asyncio
    async def test_upsert_materialized_view(self, mock_env, sample_materialized_view):
        """Test upsert_materialized_view method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_materialized_view
            
            result = await client.upsert_materialized_view(
                name="dune.team.result_daily",
                query_id=1215383,
                cron_expression="0 */1 * * *",
                performance="medium",
            )
            
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            assert call_args[0][0] == "POST"
            assert call_args[1]["json_data"]["query_id"] == 1215383

    @pytest.mark.asyncio
    async def test_delete_materialized_view(self, mock_env):
        """Test delete_materialized_view method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            await client.delete_materialized_view("dune.team.result_daily")
            
            mock_request.assert_called_once_with(
                "DELETE",
                "/materialized-views/dune.team.result_daily",
            )

    @pytest.mark.asyncio
    async def test_list_materialized_views(self, mock_env):
        """Test list_materialized_views method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"views": []}
            
            await client.list_materialized_views(limit=20)
            
            mock_request.assert_called_once_with(
                "GET",
                "/materialized-views",
                params={"limit": 20},
            )

    @pytest.mark.asyncio
    async def test_refresh_materialized_view(self, mock_env, sample_execution_response):
        """Test refresh_materialized_view method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_execution_response
            
            result = await client.refresh_materialized_view(
                "dune.team.result_daily",
                performance="large",
            )
            
            mock_request.assert_called_once()
            assert result["execution_id"] == "01HKZJ2683PHF9Q9PHHQ8FW4Q1"


class TestDuneClientTables:
    """Tests for table/upload methods."""

    @pytest.mark.asyncio
    async def test_create_table(self, mock_env):
        """Test create_table method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            schema = [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "varchar"},
            ]
            await client.create_table("my_ns", "my_table", schema)
            
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            assert call_args[1]["json_data"]["table_name"] == "my_table"

    @pytest.mark.asyncio
    async def test_insert_data(self, mock_env):
        """Test insert_data method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            data = [{"id": 1, "name": "test"}]
            await client.insert_data("my_ns", "my_table", data)
            
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            assert "/uploads/my_ns/my_table/insert" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_clear_table(self, mock_env):
        """Test clear_table method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            await client.clear_table("my_ns", "my_table")
            
            mock_request.assert_called_once_with(
                "POST",
                "/uploads/my_ns/my_table/clear",
            )

    @pytest.mark.asyncio
    async def test_delete_table(self, mock_env):
        """Test delete_table method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"success": True}
            
            await client.delete_table("my_ns", "my_table")
            
            mock_request.assert_called_once_with(
                "DELETE",
                "/uploads/my_ns/my_table",
            )

    @pytest.mark.asyncio
    async def test_list_tables(self, mock_env):
        """Test list_tables method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"tables": []}
            
            await client.list_tables()
            
            mock_request.assert_called_once_with(
                "GET",
                "/uploads",
                params=None,
            )


class TestDuneClientDatasets:
    """Tests for dataset methods."""

    @pytest.mark.asyncio
    async def test_get_dataset(self, mock_env, sample_dataset_info):
        """Test get_dataset method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_dataset_info
            
            result = await client.get_dataset("ethereum", "transactions")
            
            mock_request.assert_called_once_with(
                "GET",
                "/datasets/ethereum/transactions",
            )
            assert result["namespace"] == "ethereum"

    @pytest.mark.asyncio
    async def test_list_datasets(self, mock_env):
        """Test list_datasets method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"datasets": []}
            
            await client.list_datasets(owner="dune", limit=50)
            
            mock_request.assert_called_once_with(
                "GET",
                "/datasets",
                params={"owner": "dune", "limit": 50},
            )


class TestDuneClientPipelines:
    """Tests for pipeline methods."""

    @pytest.mark.asyncio
    async def test_execute_pipeline(self, mock_env, sample_execution_response):
        """Test execute_pipeline method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_execution_response
            
            result = await client.execute_pipeline("team/my-pipeline", "large")
            
            mock_request.assert_called_once_with(
                "POST",
                "/pipelines/team/my-pipeline/execute",
                json_data={"performance": "large"},
            )
            assert result["execution_id"] == "01HKZJ2683PHF9Q9PHHQ8FW4Q1"

    @pytest.mark.asyncio
    async def test_get_pipeline_status(self, mock_env, sample_execution_status):
        """Test get_pipeline_status method."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_execution_status
            
            result = await client.get_pipeline_status("exec-123")
            
            mock_request.assert_called_once_with(
                "GET",
                "/pipelines/exec-123/status",
            )
            assert result["state"] == "QUERY_STATE_COMPLETED"


class TestDuneClientUsage:
    """Tests for usage methods."""

    @pytest.mark.asyncio
    async def test_get_usage(self, mock_env, sample_usage_info):
        """Test get_usage method without date params."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_usage_info
            
            result = await client.get_usage()
            
            mock_request.assert_called_once_with(
                "POST",
                "/usage",
                json_data=None,
            )
            assert result["credits_used"] == 1500.5

    @pytest.mark.asyncio
    async def test_get_usage_with_dates(self, mock_env, sample_usage_info):
        """Test get_usage method with date parameters."""
        client = DuneClient()
        
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_usage_info
            
            result = await client.get_usage("2024-01-01", "2024-02-01")
            
            mock_request.assert_called_once_with(
                "POST",
                "/usage",
                json_data={"start_date": "2024-01-01", "end_date": "2024-02-01"},
            )


class TestDuneAPIError:
    """Tests for DuneAPIError exception."""

    def test_error_message(self):
        """Test error message formatting."""
        error = DuneAPIError(400, "Bad Request")
        assert error.status_code == 400
        assert error.message == "Bad Request"
        assert "400" in str(error)
        assert "Bad Request" in str(error)
