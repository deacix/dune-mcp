"""
Pytest fixtures for Dune MCP tests.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mock_api_key():
    """Provide a mock API key."""
    return "test-api-key-12345"


@pytest.fixture
def mock_env(mock_api_key):
    """Mock environment with DUNE_API_KEY."""
    with patch.dict(os.environ, {"DUNE_API_KEY": mock_api_key}):
        yield


@pytest.fixture
def mock_httpx_client():
    """Create a mock httpx AsyncClient."""
    mock_client = AsyncMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": True}
    mock_response.text = '{"success": true}'
    mock_response.headers = {"content-type": "application/json"}
    mock_client.request.return_value = mock_response
    mock_client.post.return_value = mock_response
    mock_client.get.return_value = mock_response
    return mock_client


@pytest.fixture
def sample_execution_response():
    """Sample execution response from Dune API."""
    return {
        "execution_id": "01HKZJ2683PHF9Q9PHHQ8FW4Q1",
        "state": "QUERY_STATE_PENDING",
    }


@pytest.fixture
def sample_query_result():
    """Sample query result from Dune API."""
    return {
        "execution_id": "01HKZJ2683PHF9Q9PHHQ8FW4Q1",
        "query_id": 1215383,
        "state": "QUERY_STATE_COMPLETED",
        "result": {
            "rows": [
                {"date": "2024-01-01", "volume": 1000000},
                {"date": "2024-01-02", "volume": 1500000},
            ],
            "metadata": {
                "column_names": ["date", "volume"],
                "column_types": ["date", "double"],
                "row_count": 2,
            },
        },
    }


@pytest.fixture
def sample_execution_status():
    """Sample execution status from Dune API."""
    return {
        "execution_id": "01HKZJ2683PHF9Q9PHHQ8FW4Q1",
        "query_id": 1215383,
        "state": "QUERY_STATE_COMPLETED",
        "submitted_at": "2024-01-15T10:00:00Z",
        "started_at": "2024-01-15T10:00:01Z",
        "ended_at": "2024-01-15T10:00:05Z",
        "result_set_bytes": 1024,
        "total_row_count": 100,
    }


@pytest.fixture
def sample_query_info():
    """Sample query info from Dune API."""
    return {
        "query_id": 1215383,
        "name": "Test Query",
        "description": "A test query",
        "query_sql": "SELECT * FROM ethereum.transactions LIMIT 10",
        "is_private": True,
        "is_archived": False,
        "tags": ["test", "ethereum"],
        "parameters": [],
    }


@pytest.fixture
def sample_materialized_view():
    """Sample materialized view info from Dune API."""
    return {
        "name": "dune.team.result_daily_volume",
        "query_id": 1215383,
        "cron_expression": "0 */1 * * *",
        "performance": "medium",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-15T00:00:00Z",
    }


@pytest.fixture
def sample_table_info():
    """Sample table info from Dune API."""
    return {
        "namespace": "my_namespace",
        "table_name": "my_table",
        "full_name": "dune.my_namespace.my_table",
        "created_at": "2024-01-01T00:00:00Z",
    }


@pytest.fixture
def sample_dataset_info():
    """Sample dataset info from Dune API."""
    return {
        "namespace": "ethereum",
        "name": "transactions",
        "description": "Ethereum transactions",
        "columns": [
            {"name": "hash", "type": "varchar"},
            {"name": "block_number", "type": "bigint"},
            {"name": "value", "type": "uint256"},
        ],
    }


@pytest.fixture
def sample_usage_info():
    """Sample usage info from Dune API."""
    return {
        "credits_used": 1500.5,
        "credits_remaining": 8499.5,
        "period_start": "2024-01-01T00:00:00Z",
        "period_end": "2024-02-01T00:00:00Z",
    }


@pytest.fixture
def sample_csv_result():
    """Sample CSV result from Dune API."""
    return "date,volume\n2024-01-01,1000000\n2024-01-02,1500000\n"
