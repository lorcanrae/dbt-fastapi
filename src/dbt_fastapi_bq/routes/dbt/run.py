from fastapi import APIRouter, Depends, Path
from typing import Literal, Annotated

from dbt_fastapi_bq.models.dbt_models import (
    DbtRunRequest,
    DbtRunResult,
    BaseDbtError,
    DbtModelSelectionError,
)
from dbt_fastapi_bq.utils.payload_validators import validate_dbt_run_request
from dbt_fastapi_bq.utils.dbt_utils import execute_dbt_command, generate_dbt_command


router = APIRouter()

DBTCommand = Literal["run", "test", "build"]


@router.post(
    "/run",
    response_model=DbtRunResult,
    responses={
        400: {
            "model": DbtModelSelectionError,
            "Description": "Invalid dbt model or target",
        },
        500: {"model": BaseDbtError, "Description": "Internal dbt command failure"},
    },
)
async def run_dbt(
    command: Annotated[DBTCommand, Path(..., description="dbt command to execute")],
    payload: Annotated[DbtRunRequest, Depends(validate_dbt_run_request)],
) -> dict[str, str]:
    dbt_cmd = generate_dbt_command("run", payload)
    stdout: str = execute_dbt_command(
        dbt_cmd, target=payload.target, model=payload.model
    )

    return {"status": "success", "output": stdout}
