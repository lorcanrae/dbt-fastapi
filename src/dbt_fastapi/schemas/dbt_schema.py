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


class DbtRunTestCompileSeedSnapshotDocs(DbtCommandRequestBase):
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


class DbtCommandResponse(BaseModel):
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


class DbtListResponse(BaseModel):
    """
    Response model for 'list' endpoint
    """

    status: Literal["success"] = Field(
        default="success", description="Execution status"
    )
    nodes: list[str] = Field(
        default_factory=list,
        description="List of dbt nodes matching the selection criteria",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata about the list operation"
    )


# class DbtNode(BaseModel):
#     """
#     Represents a dbt node (model, test, seed, etc.)
#     """

#     name: str = Field(..., description="Node name")
#     resource_type: str = Field(
#         ..., description="Type of resource (model, test, seed, etc.)"
#     )
#     package_name: str = Field(..., description="Package containing the node")
#     path: Optional[str] = Field(None, description="File path relative to project root")
#     unique_id: str = Field(..., description="Unique identifier for the node")
#     depends_on: list[str] = Field(
#         default_factory=list, description="List of node dependencies"
#     )
#     description: Optional[str] = Field(None, description="Node description")
#     tags: list[str] = Field(default_factory=list, description="Node tags")
#     config: dict[str, Any] = Field(
#         default_factory=dict, description="Node configuration"
#     )
