import os
from pydantic import BaseModel, StringConstraints, Field
from typing import Optional, Annotated


ValidatedModelStr = Annotated[
    str, StringConstraints(min_length=1, pattern=r"^[a-zA-Z0-9_]+$")
]


class DbtRunRequest(BaseModel):
    model: Optional[ValidatedModelStr] = None
    upstream: bool = False
    downstream: bool = False
    target: ValidatedModelStr = Field(default_factory=os.getenv("DBT_TARGET", "dev"))


class DbtRunResult(BaseModel):
    status: str
    output: str
