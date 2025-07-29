from pydantic import BaseModel, StringConstraints, Field, model_validator
from typing import Optional, Annotated, Literal, Any


# Annotated type
ValidatedModelStr = Annotated[
    str, StringConstraints(min_length=1, pattern=r"^[a-zA-Z0-9_]+$")
]


### run/test/build endpoints
class DbtCommandRequest(BaseModel):
    target: ValidatedModelStr = Field(..., description="dbt target (e.g. dev, prod)")
    select_model: Optional[ValidatedModelStr] = None
    select_upstream: bool = False
    select_downstream: bool = False
    exclude_model: Optional[ValidatedModelStr] = None
    exclude_upstream: bool = False
    exclude_downstream: bool = False

    @model_validator(mode="after")
    def validate_upstream_downsream(self):
        # select_model
        if (self.select_upstream or self.select_downstream) and not self.select_model:
            raise ValueError(
                "A valid 'select_model' parameter must be provided if 'select_upstream' or 'select_downstream' is True."
            )

        # exclude_model
        if (
            self.exclude_upstream or self.exclude_downstream
        ) and not self.exclude_model:
            raise ValueError(
                "A valid 'exclude_model' parameter must be provided if 'exclude_upstream' or 'exclude_downstream' is True."
            )

        return self


class DbtCommandResponse(BaseModel):
    status: Literal["success", "partial_failure"] = Field(
        ..., description="Execution status, e.g. success or failure"
    )
    output: str = Field(..., description="Raw dbt CLI output")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Optional metadata"
    )
