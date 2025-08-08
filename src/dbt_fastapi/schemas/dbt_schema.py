from pydantic import BaseModel, StringConstraints, Field, model_validator
from typing import Optional, Annotated, Literal, Any
import shlex


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
    metadata: dict[str, Any] = Field(
        default_factory=list,
        description="Additional metadata about the executed dbt command.",
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
