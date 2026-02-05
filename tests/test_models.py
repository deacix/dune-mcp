"""
Unit tests for Pydantic models.
"""

import pytest
from pydantic import ValidationError

from dune.dune_mcp.models import (
    CreateQueryRequest,
    CreateQueryResponse,
    CreateTableRequest,
    DatasetInfo,
    ExecuteQueryResponse,
    ExecutionResult,
    ExecutionStatus,
    MaterializedViewInfo,
    QueryInfo,
    QueryParameter,
    TableInfo,
    UpsertMaterializedViewRequest,
    UsageInfo,
)


class TestQueryParameter:
    """Tests for QueryParameter model."""

    def test_basic_parameter(self):
        """Test creating a basic parameter."""
        param = QueryParameter(key="address", value="0x123", type="text")
        assert param.key == "address"
        assert param.value == "0x123"
        assert param.type == "text"

    def test_parameter_with_enum_options(self):
        """Test parameter with enum options."""
        param = QueryParameter(
            key="chain",
            value="ethereum",
            type="enum",
            enumOptions=["ethereum", "polygon"],
        )
        assert param.enum_options == ["ethereum", "polygon"]

    def test_parameter_default_type(self):
        """Test parameter default type."""
        param = QueryParameter(key="test", value="val")
        assert param.type == "text"


class TestExecuteQueryResponse:
    """Tests for ExecuteQueryResponse model."""

    def test_valid_response(self):
        """Test valid response."""
        response = ExecuteQueryResponse(
            execution_id="exec-123",
            state="QUERY_STATE_PENDING",
        )
        assert response.execution_id == "exec-123"
        assert response.state == "QUERY_STATE_PENDING"


class TestExecutionStatus:
    """Tests for ExecutionStatus model."""

    def test_complete_status(self):
        """Test complete status with all fields."""
        status = ExecutionStatus(
            execution_id="exec-123",
            query_id=1215383,
            state="QUERY_STATE_COMPLETED",
            submitted_at="2024-01-15T10:00:00Z",
            started_at="2024-01-15T10:00:01Z",
            ended_at="2024-01-15T10:00:05Z",
            result_set_bytes=1024,
            total_row_count=100,
        )
        assert status.execution_id == "exec-123"
        assert status.query_id == 1215383
        assert status.total_row_count == 100

    def test_minimal_status(self):
        """Test minimal status with required fields only."""
        status = ExecutionStatus(
            execution_id="exec-123",
            query_id=1215383,
            state="QUERY_STATE_PENDING",
        )
        assert status.submitted_at is None
        assert status.result_set_bytes is None


class TestExecutionResult:
    """Tests for ExecutionResult model."""

    def test_result_with_data(self):
        """Test result with data."""
        result = ExecutionResult(
            execution_id="exec-123",
            query_id=1215383,
            state="QUERY_STATE_COMPLETED",
            result={
                "rows": [{"col1": "val1"}],
                "metadata": {"row_count": 1},
            },
        )
        assert result.result is not None
        assert len(result.result["rows"]) == 1

    def test_result_without_data(self):
        """Test result without data (pending)."""
        result = ExecutionResult(
            execution_id="exec-123",
            query_id=1215383,
            state="QUERY_STATE_PENDING",
        )
        assert result.result is None


class TestCreateQueryRequest:
    """Tests for CreateQueryRequest model."""

    def test_valid_request(self):
        """Test valid create query request."""
        request = CreateQueryRequest(
            name="Test Query",
            query_sql="SELECT 1",
            description="A test query",
            is_private=True,
            tags=["test"],
        )
        assert request.name == "Test Query"
        assert request.is_private is True

    def test_minimal_request(self):
        """Test minimal required fields."""
        request = CreateQueryRequest(
            name="Test",
            query_sql="SELECT 1",
        )
        assert request.description is None
        assert request.is_private is True  # default

    def test_missing_required_fields(self):
        """Test validation error for missing fields."""
        with pytest.raises(ValidationError):
            CreateQueryRequest(name="Test")  # missing query_sql


class TestCreateQueryResponse:
    """Tests for CreateQueryResponse model."""

    def test_valid_response(self):
        """Test valid response."""
        response = CreateQueryResponse(query_id=12345)
        assert response.query_id == 12345


class TestQueryInfo:
    """Tests for QueryInfo model."""

    def test_complete_info(self):
        """Test complete query info."""
        info = QueryInfo(
            query_id=1215383,
            name="Test Query",
            description="Description",
            query_sql="SELECT 1",
            is_private=True,
            is_archived=False,
            tags=["test"],
            parameters=[],
        )
        assert info.query_id == 1215383
        assert info.is_archived is False

    def test_minimal_info(self):
        """Test minimal query info."""
        info = QueryInfo(query_id=1, name="Test")
        assert info.description is None


class TestMaterializedViewInfo:
    """Tests for MaterializedViewInfo model."""

    def test_complete_info(self):
        """Test complete materialized view info."""
        info = MaterializedViewInfo(
            name="dune.team.result_daily",
            query_id=1215383,
            cron_expression="0 */1 * * *",
            performance="medium",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-15T00:00:00Z",
        )
        assert info.name == "dune.team.result_daily"
        assert info.performance == "medium"


class TestUpsertMaterializedViewRequest:
    """Tests for UpsertMaterializedViewRequest model."""

    def test_valid_request(self):
        """Test valid upsert request."""
        request = UpsertMaterializedViewRequest(
            name="dune.team.result_daily",
            query_id=1215383,
            cron_expression="0 */1 * * *",
            performance="large",
        )
        assert request.name == "dune.team.result_daily"
        assert request.performance == "large"

    def test_default_performance(self):
        """Test default performance value."""
        request = UpsertMaterializedViewRequest(
            name="dune.team.result_daily",
            query_id=1215383,
        )
        assert request.performance == "medium"


class TestTableInfo:
    """Tests for TableInfo model."""

    def test_complete_info(self):
        """Test complete table info."""
        info = TableInfo(
            namespace="my_ns",
            table_name="my_table",
            full_name="dune.my_ns.my_table",
            created_at="2024-01-01T00:00:00Z",
        )
        assert info.full_name == "dune.my_ns.my_table"


class TestDatasetInfo:
    """Tests for DatasetInfo model."""

    def test_complete_info(self):
        """Test complete dataset info."""
        info = DatasetInfo(
            namespace="ethereum",
            name="transactions",
            description="Ethereum transactions",
            columns=[
                {"name": "hash", "type": "varchar"},
                {"name": "value", "type": "uint256"},
            ],
        )
        assert info.namespace == "ethereum"
        assert len(info.columns) == 2


class TestUsageInfo:
    """Tests for UsageInfo model."""

    def test_complete_info(self):
        """Test complete usage info."""
        info = UsageInfo(
            credits_used=1500.5,
            credits_remaining=8499.5,
            period_start="2024-01-01T00:00:00Z",
            period_end="2024-02-01T00:00:00Z",
        )
        assert info.credits_used == 1500.5
        assert info.credits_remaining == 8499.5

    def test_minimal_info(self):
        """Test minimal usage info."""
        info = UsageInfo()
        assert info.credits_used is None
