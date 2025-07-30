from pydantic import BaseModel, StringConstraints, Field, model_validator
from typing import Optional, Annotated, Literal, Any
import shlex


# ====== Request Schema =====

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

    def get_quoted_select(self) -> Optional[str]:
        return shlex.quote(self.select_args) if self.select_args else None

    def get_quoted_exclude(self) -> Optional[str]:
        return shlex.quote(self.exclude_args) if self.exclude_args else None

    def get_quoted_selector(self) -> Optional[str]:
        return shlex.quote(self.selector_args) if self.selector_args else None

    @model_validator(mode="after")
    def validate_mutually_exclusive_args(self):
        if (self.select_args or self.exclude_args) and self.selector_args:
            raise ValueError(
                "'select_args' and 'exclude_args' are mutually exclusive with 'selector_args'"
            )
        return self


class DbtRunTestRequest(DbtCommandRequestBase):
    defer: Optional[bool] = False


class DbtListRequest(DbtCommandRequestBase):
    resource_type: Optional[str] = None


class DbtBuildRequest(DbtCommandRequestBase):
    resource_type: Optional[str] = None
    defer: Optional[bool] = False


class DbtCompileSeedSnapshotDocs(DbtCommandRequestBase):
    pass


class DbtUnsafeRequest(BaseModel):
    unsafe_dbt_cli_command: str


# ===== Response Schema =====


class DbtCommandResponse(BaseModel):
    status: Literal["success", "partial_failure"] = Field(
        ..., description="Execution status, e.g. success or failure"
    )
    output: str = Field(..., description="Raw dbt CLI output")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Optional metadata"
    )
