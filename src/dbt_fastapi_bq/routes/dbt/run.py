from fastapi import APIRouter, Depends
import os

from dbt_fastapi_bq.models.dbt_models import (
    DbtRunRequest,
    DbtRunResult,
    BaseDbtError,
    DbtModelSelectionError,
)
from dbt_fastapi_bq.utils.payload_validators import validate_dbt_run_request
from dbt_fastapi_bq.utils.dbt_executor import execute_dbt_command
from dbt_fastapi_bq.params import DBT_PROJECT_PATH

router = APIRouter()


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
    payload: DbtRunRequest = Depends(validate_dbt_run_request),
) -> dict[str, str]:
    command: list[str] = ["dbt", "run", "--project-dir", DBT_PROJECT_PATH]
    print(DBT_PROJECT_PATH)
    if payload.model:
        select_arg = payload.model
        if payload.upstream:
            select_arg = f"+{select_arg}"
        if payload.downstream:
            select_arg = f"{select_arg}+"
        command += ["--select", select_arg]

    command += ["--target", payload.target]

    stdout: str = execute_dbt_command(
        command, target=payload.target, model=payload.model
    )

    return {"status": "success", "output": stdout}


@router.get("/runtest")
def run_test():
    output = os.getenv("DBT_TARGET", "not found")
    return {"output": output}
