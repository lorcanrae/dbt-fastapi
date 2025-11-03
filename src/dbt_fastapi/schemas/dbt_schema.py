from pydantic import BaseModel, StringConstraints, Field, model_validator
from typing import Optional, Annotated, Literal, Any
import shlex

from enum import Enum

# === Request Schemas ===


# Annotated type
ValidatedModelStr = Annotated[
    str, StringConstraints(min_length=1, pattern=r"^[a-zA-Z0-9_]+$")
]


class DbtCommandRequestBase(BaseModel):
    target: ValidatedModelStr = Field(
        ..., description="dbt target (e.g. dev, prod, test)"
    )
    select_args: Optional[str] = None
    exclude_args: Optional[str] = None
    selector_args: Optional[str] = None

    @model_validator(mode="after")
    def validate_mutually_exclusive_args(self):
        if (self.select_args or self.exclude_args) and self.selector_args:
            raise ValueError(
                "'select_args' and 'exclude_args' are mutually exclusive with 'selector_args'"
            )
        return self


class DbtBuildListRequest(DbtCommandRequestBase):
    resource_type: Optional[str] = None


class DbtRunTestCompileSeedSnapshotDocsRequest(DbtCommandRequestBase):
    pass


class DbtUnsafeRequest(BaseModel):
    unsafe_dbt_cli_command: str

    @model_validator(mode="after")
    def sanitize_input(self):
        illegal_tokens = ["&&", "|", ";", "$(", "<", ">"]

        for token in illegal_tokens:
            if token in self.unsafe_dbt_cli_command:
                raise ValueError(f"Illegal token '{token}' found in input CLI command")

        shlexed_cli_command = shlex.split(self.unsafe_dbt_cli_command)

        if not shlexed_cli_command or shlexed_cli_command[0] != "dbt":
            raise ValueError("Command must start with 'dbt'")

        if any("dbt" in arg for arg in shlexed_cli_command[1:]):
            raise ValueError("Multiple 'dbt' references detected")

        return self


# === Response Schemas ===


class ResponseTestStatus(str, Enum):
    """
    Enum for test execution status.
    Why doesn't python have enums as a native data structure?
    """

    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"
    SKIP = "skip"


class DbtTestResult(BaseModel):
    """
    Detailed information for a single dbt test.
    """

    unique_id: str = Field(..., description="Test UID")
    name: str = Field(..., description="Test name")
    status: ResponseTestStatus = Field(..., description="Test execution status")
    execution_time: Optional[float] = Field(
        None, description="Test execution time in seconds"
    )
    message: Optional[str] = Field(None, description="Error message if the test failed")
    failures: Optional[int] = Field(
        None, description="Number of failing rows in the test"
    )


class DbtNode(BaseModel):
    """
    Represents a single dbt node with its key identifiers.
    """

    unique_id: str = Field(
        ...,
        description="The full unique identifier for the node (e.g. 'model.project.table_name')",
    )
    fqn: str = Field(
        ..., description="Fully qualified name for use with: --select, --exclude"
    )
    resource_type: Optional[str] = Field(
        ..., description="Type of dbt resource (model, test, snapshot, etc.)"
    )
    depends_on: list[Optional[str]] = Field(
        default_factory=list, description="Upstream node dependencies."
    )

    # Test specific field
    test_result: Optional[DbtTestResult] = Field(
        None, description="Test execution details"
    )


class DbtResponse(BaseModel):
    """
    Unified response model for dbt commands: run, test, build, list, compile
    """

    success: bool = Field(
        ..., description="Whether the dbt command executed successfully"
    )
    nodes: list[DbtNode] = Field(
        default_factory=list,
        description="List of dbt nodes that were processed or matched the selection criteria",
    )
    # TODO: This looks wrong
    metadata: dict[str, Any] = Field(
        default_factory=list,
        description="Additional metadata about the executed dbt command.",
    )

    # Test specific
    test_summary: Optional[dict[str, int]] = Field(
        None,
        description="Summary of test results: {total, passed, failed, errored, skipped}",
    )


class DbtCommandResponse(BaseModel):
    """
    For the unsafe endpoint. Will eventually depreciate.
    """

    status: Literal["success", "partial_failure"] = Field(
        ..., description="Execution status, e.g. success or failure"
    )
    output: str = Field(..., description="Raw dbt CLI output")
    nodes: list[str] = Field(
        default_factory=list,
        description="List of dbt nodes that were processed or would be processed",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Optional metadata"
    )
