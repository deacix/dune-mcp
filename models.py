"""
Pydantic models for Dune API request/response types.
"""

from typing import Any

from pydantic import BaseModel, Field


# ============================================================================
# Query Parameters
# ============================================================================

class QueryParameter(BaseModel):
    """A parameter for a Dune query."""
    key: str
    value: str
    type: str = "text"  # text, number, date, enum
    enum_options: list[str] | None = Field(default=None, alias="enumOptions")


# ============================================================================
# Execution Responses
# ============================================================================

class ExecuteQueryResponse(BaseModel):
    """Response from executing a query."""
    execution_id: str
    state: str


class ExecutionStatus(BaseModel):
    """Status of a query execution."""
    execution_id: str
    query_id: int
    state: str
    submitted_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    execution_started_at: str | None = None
    execution_ended_at: str | None = None
    result_set_bytes: int | None = None
    total_row_count: int | None = None


class ExecutionResult(BaseModel):
    """Result of a query execution."""
    execution_id: str
    query_id: int
    state: str
    result: dict[str, Any] | None = None


# ============================================================================
# Query Management
# ============================================================================

class CreateQueryRequest(BaseModel):
    """Request to create a new query."""
    name: str
    query_sql: str
    description: str | None = None
    is_private: bool = True
    parameters: list[QueryParameter] | None = None
    tags: list[str] | None = None


class CreateQueryResponse(BaseModel):
    """Response from creating a query."""
    query_id: int


class QueryInfo(BaseModel):
    """Information about a query."""
    query_id: int
    name: str
    description: str | None = None
    query_sql: str | None = None
    is_private: bool | None = None
    is_archived: bool | None = None
    tags: list[str] | None = None
    parameters: list[dict[str, Any]] | None = None


# ============================================================================
# Materialized Views
# ============================================================================

class MaterializedViewInfo(BaseModel):
    """Information about a materialized view."""
    name: str
    query_id: int
    cron_expression: str | None = None
    performance: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class UpsertMaterializedViewRequest(BaseModel):
    """Request to create or update a materialized view."""
    name: str
    query_id: int
    cron_expression: str | None = None
    performance: str = "medium"


# ============================================================================
# Tables / Uploads
# ============================================================================

class CreateTableRequest(BaseModel):
    """Request to create a new table."""
    namespace: str
    table_name: str
    schema_: list[dict[str, str]] = Field(alias="schema")
    description: str | None = None
    is_private: bool = True


class TableInfo(BaseModel):
    """Information about an uploaded table."""
    namespace: str
    table_name: str
    full_name: str | None = None
    created_at: str | None = None


# ============================================================================
# Datasets
# ============================================================================

class DatasetInfo(BaseModel):
    """Information about a dataset."""
    namespace: str
    name: str
    description: str | None = None
    columns: list[dict[str, Any]] | None = None


# ============================================================================
# Usage
# ============================================================================

class UsageInfo(BaseModel):
    """Billing usage information."""
    credits_used: float | None = None
    credits_remaining: float | None = None
    period_start: str | None = None
    period_end: str | None = None
