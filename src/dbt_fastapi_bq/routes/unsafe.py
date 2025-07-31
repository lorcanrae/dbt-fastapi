from fastapi import APIRouter
import shlex

from dbt_fastapi_bq.dbt_manager import DbtManager

from dbt_fastapi_bq.schemas.dbt_schema import (
    DbtUnsafeRequest,
    DbtCommandResponse,
)

router = APIRouter()

COMMAND = "unsafe"


@router.post(
    f"/{COMMAND}",
    summary="Execute arbitrary dbt command. Requiers the entire dbt CLI command to be executed. Limited error handling.",
)
async def run_dbt(
    payload: DbtUnsafeRequest,
) -> DbtCommandResponse:
    shlex_list_cli = shlex.split(payload.unsafe_dbt_cli_command)

    output = DbtManager.execute_unsafe_dbt_command(shlex_list_cli)

    metadata = {"dbt_command": payload.unsafe_dbt_cli_command}

    return DbtCommandResponse(status="success", output=output, metadata=metadata)
