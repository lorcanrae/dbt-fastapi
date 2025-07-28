from fastapi import APIRouter, Depends
from typing import Annotated

from dbt_fastapi_bq.models.dbt_models import (
    DbtCommandRequest,
    DbtCommandResult,
    BaseDbtError,
    DbtModelSelectionError,
)
from dbt_fastapi_bq.utils import (
    execute_dbt_command,
    generate_dbt_command,
    validate_dbt_command_request,
)


router = APIRouter()

COMMAND = "build"


@router.post(
    f"/{COMMAND}",
    response_model=DbtCommandResult,
    responses={
        400: {
            "model": DbtModelSelectionError,
            "Description": "Invalid dbt model or target",
        },
        500: {"model": BaseDbtError, "Description": "Internal dbt command failure"},
    },
    summary=f"Execute 'dbt {COMMAND}'",
)
async def run_dbt(
    payload: Annotated[DbtCommandRequest, Depends(validate_dbt_command_request)],
) -> dict[str, str]:
    dbt_cmd = generate_dbt_command(COMMAND, payload)
    stdout: str = execute_dbt_command(dbt_cmd, payload=payload)

    return {"status": "success", "output": stdout}
