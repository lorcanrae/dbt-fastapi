from pydantic import BaseModel, StringConstraints, Field, model_validator
from typing import Optional, Annotated
from dbt_fastapi.schemas.enums import DbtResourceType
import shlex

# === Type Definitions ===

ValidatedTargetStr = Annotated[
    str, StringConstraints(min_length=1, pattern=r"^[a-zA-Z0-9_]+$")
]


# === Request Schemas - Base ===


class DbtCommandRequestBase(BaseModel):
    """
    Base request schema with common fields across all dbt commands.

    All dbt commands support node selection via select/exclude/selector args.
    """

    target: Optional[ValidatedTargetStr] = Field(
        None,
        description="dbt target profile (e.g., 'dev', 'prod'). Uses default if not specified.",
    )
    select_args: Optional[str] = Field(
        None,
        description="Space-separated node selection criteria (e.g., 'model1 tag:daily')",
        examples=["model1 model2", "tag:daily", "+model1+"],
    )
    exclude_args: Optional[str] = Field(
        None,
        description="Space-separated node exclusion criteria",
        examples=["model3", "tag:deprecated"],
    )
    selector_args: Optional[str] = Field(
        None,
        description="Named selector from selectors.yml",
        examples=["my_selector"],
    )

    @model_validator(mode="after")
    def validate_mutually_exclusive_args(self) -> "DbtCommandRequestBase":
        """Ensure select/exclude and selector args are not used together."""
        if (self.select_args or self.exclude_args) and self.selector_args:
            raise ValueError(
                "'select_args' and 'exclude_args' are mutually exclusive with 'selector_args'"
            )
        return self


# === Request Schemas - Commands ===


class DbtRunRequest(DbtCommandRequestBase):
    """Request schema for dbt run command."""

    full_refresh: bool = Field(
        False,
        description="If True, treat incremental models as table models (full refresh)",
    )
    fail_fast: bool = Field(
        False,
        description="If True, stop execution on first failure",
    )


class DbtTestRequest(DbtCommandRequestBase):
    """Request schema for dbt test command."""

    store_failures: bool = Field(
        False,
        description="If True, store test failures in the database for analysis",
    )
    pass_on_test_failures: bool = Field(
        False,
        description="If True, return HTTP 200 when internal command succeeds with `dbt test` failures or errors",
    )


class DbtBuildRequest(DbtCommandRequestBase):
    """
    Request schema for dbt build command.

    Combines run, test, snapshot, and seed operations in dependency order.
    Supports options from both run and test commands.
    """

    full_refresh: bool = Field(
        False,
        description="If True, treat incremental models as table models (full refresh)",
    )
    store_failures: bool = Field(
        False,
        description="If True, store test failures in the database for analysis",
    )
    fail_fast: bool = Field(
        False,
        description="If True, stop execution on first failure",
    )
    pass_on_test_failures: bool = Field(
        False,
        description="If True, return HTTP 200 when internal command succeeds with `dbt test` failures or errors",
    )


class DbtListRequest(DbtCommandRequestBase):
    """Request schema for dbt list command."""

    resource_type: Optional[DbtResourceType] = Field(
        None,
        description="Filter results by resource type",
    )


class DbtCompileRequest(DbtCommandRequestBase):
    """Request schema for dbt compile command."""

    pass


class DbtUnsafeRequest(BaseModel):
    """
    Request schema for unsafe/raw dbt CLI command execution.

    WARNING: This endpoint provides direct CLI access with minimal validation.
    Use command-specific endpoints when possible.
    """

    unsafe_dbt_cli_command: str = Field(
        ...,
        description="Complete dbt CLI command as a string (e.g., 'dbt run --select model1')",
        examples=["dbt run --select model1", "dbt test --select tag:critical"],
    )

    @model_validator(mode="after")
    def sanitize_input(self) -> "DbtUnsafeRequest":
        """Validate and sanitize the unsafe CLI command."""
        illegal_tokens = ["&&", "|", ";", "$(", "<", ">"]

        for token in illegal_tokens:
            if token in self.unsafe_dbt_cli_command:
                raise ValueError(
                    f"Illegal token '{token}' found in CLI command. "
                    f"Shell operators are not allowed."
                )

        shlexed_cli_command = shlex.split(self.unsafe_dbt_cli_command)

        if not shlexed_cli_command or shlexed_cli_command[0] != "dbt":
            raise ValueError("Command must start with 'dbt'")

        if any("dbt" in arg for arg in shlexed_cli_command[1:]):
            raise ValueError("Multiple 'dbt' references detected in command")

        return self
