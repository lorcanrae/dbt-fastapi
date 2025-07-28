from pydantic import BaseModel, StringConstraints, Field
from typing import Optional, Annotated


# Annotated type
ValidatedModelStr = Annotated[
    str, StringConstraints(min_length=1, pattern=r"^[a-zA-Z0-9_]+$")
]


### run/test/build endpoints
class DbtCommandRequest(BaseModel):
    target: ValidatedModelStr = Field(..., description="dbt target (e.g. dev, prod)")
    select_model: Optional[ValidatedModelStr] = None
    upstream: bool = False
    downstream: bool = False


class DbtCommandResult(BaseModel):
    status: str
    output: str


### Error Responses
class BaseDbtError(BaseModel):
    error: str
    message: str


class DbtModelSelectionError(BaseDbtError):
    provided_model: str


class DbtTargetValidationError(BaseDbtError):
    provided_target: str
    valid_targets: list[str]
