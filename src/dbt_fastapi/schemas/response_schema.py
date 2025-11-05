from pydantic import BaseModel, Field
from typing import Optional, Literal, Any

from dbt_fastapi.schemas.enums import ResponseTestStatus


# === Response Schemas - Nested Objects ===


class DbtTestResult(BaseModel):
    """Detailed information for a single dbt test execution."""

    unique_id: str = Field(
        ...,
        description="Unique identifier for the test (e.g., 'test.project.unique_users_id')",
    )
    name: str = Field(
        ...,
        description="Human-readable test name",
    )
    status: ResponseTestStatus = Field(
        ...,
        description="Test execution status",
    )
    execution_time: Optional[float] = Field(
        None,
        description="Test execution time in seconds",
    )
    message: Optional[str] = Field(
        None,
        description="Error or warning message if test did not pass",
    )
    failures: Optional[int] = Field(
        None,
        description="Number of failing rows (for data tests)",
    )


class DbtNode(BaseModel):
    """
    Represents a single dbt node (model, test, seed, snapshot, etc.).

    Contains identifiers and metadata needed to understand node relationships
    and execution results.
    """

    unique_id: str = Field(
        ...,
        description="Full unique identifier (e.g., 'model.project.table_name')",
    )
    fqn: str = Field(
        ...,
        description="Fully qualified name for use with --select/--exclude",
    )
    resource_type: str = Field(
        ...,
        description="Type of dbt resource (model, test, snapshot, seed, etc.)",
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description="List of upstream node dependencies (unique_ids)",
    )
    test_result: Optional[DbtTestResult] = Field(
        None,
        description="Test execution details (only present for test resources)",
    )


# === Response Schemas - Metadata ===


class DbtMetadataBase(BaseModel):
    """Common metadata fields across all dbt command responses."""

    command: str = Field(
        ...,
        description="The dbt command that was executed (run, test, build, etc.)",
    )
    dbt_command: str = Field(
        ...,
        description="Full dbt CLI command that was executed",
    )
    target: str = Field(
        ...,
        description="The dbt target profile used for execution",
    )
    nodes_processed: int = Field(
        ...,
        description="Total number of nodes processed by the command",
    )
    selection_criteria: str = Field(
        ...,
        description="Node selection criteria used (select/exclude/selector)",
    )


class DbtTestBuildMetadata(DbtMetadataBase):
    """Extended metadata for commands that execute tests (test, build)."""

    has_test_failures: bool = Field(
        ...,
        description="True if any tests failed",
    )
    has_test_errors: bool = Field(
        ...,
        description="True if any tests encountered errors during execution",
    )


# === Response Schemas - Base ===


class DbtResponseBase(BaseModel):
    """Base response schema with common fields across all dbt commands."""

    success: bool = Field(
        ...,
        description="Whether the dbt command executed successfully (note: dbt test failures still return success=True)",
    )


# === Response Schemas - Commands ===


class DbtRunResponse(DbtResponseBase):
    """Response schema for dbt run command."""

    nodes: list[DbtNode] = Field(
        ...,
        description="List of models that were executed",
    )
    metadata: DbtMetadataBase = Field(
        ...,
        description="Execution metadata and diagnostics",
    )


class DbtTestResponse(DbtResponseBase):
    """Response schema for dbt test command."""

    nodes: list[DbtNode] = Field(
        ...,
        description="List of tests that were executed (includes test_result field)",
    )
    metadata: DbtTestBuildMetadata = Field(
        ...,
        description="Execution metadata including test failure indicators",
    )
    test_summary: dict[str, int] = Field(
        ...,
        description="Summary statistics: {total, passed, warned, failed, errored, skipped}",
    )


class DbtBuildResponse(DbtResponseBase):
    """
    Response schema for dbt build command.

    Note: test_summary is optional because build may not include any tests
    depending on node selection.
    """

    nodes: list[DbtNode] = Field(
        ...,
        description="List of all nodes executed (models, tests, seeds, snapshots)",
    )
    metadata: DbtTestBuildMetadata = Field(
        ...,
        description="Execution metadata including test failure indicators",
    )
    test_summary: Optional[dict[str, int]] = Field(
        None,
        description="Summary statistics if tests were included: {total, passed, warned, failed, errored, skipped}",
    )


class DbtListResponse(DbtResponseBase):
    """Response schema for dbt list command."""

    nodes: list[DbtNode] = Field(
        ...,
        description="List of nodes matching selection criteria",
    )
    metadata: DbtMetadataBase = Field(
        ...,
        description="Execution metadata and diagnostics",
    )


class DbtCompileResponse(DbtResponseBase):
    """Response schema for dbt compile command."""

    nodes: list[DbtNode] = Field(
        ...,
        description="List of nodes that were compiled",
    )
    metadata: DbtMetadataBase = Field(
        ...,
        description="Execution metadata and diagnostics",
    )


class DbtUnsafeResponse(BaseModel):
    """
    Response schema for unsafe/raw dbt CLI command execution.

    This is a legacy response format maintained for backward compatibility.
    """

    status: Literal["success", "partial_failure"] = Field(
        ...,
        description="Execution status",
    )
    output: str = Field(
        ...,
        description="Raw dbt CLI output",
    )
    nodes: list[str] = Field(
        default_factory=list,
        description="List of node unique_ids (if extractable)",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional execution metadata",
    )
