from fastapi import APIRouter
import shlex

from dbt_fastapi.dbt_manager import DbtManager

from dbt_fastapi.schemas.request_schema import DbtUnsafeRequest
from dbt_fastapi.schemas.response_schema import DbtUnsafeResponse


router = APIRouter()

COMMAND = "unsafe"


@router.post(
    f"/{COMMAND}",
    summary="Execute arbitrary dbt command. Requires the entire dbt CLI command to be executed. Limited error handling.",
    response_model=DbtUnsafeResponse,
)
def run_dbt(
    payload: DbtUnsafeRequest,
) -> DbtUnsafeResponse:
    shlex_list_cli = shlex.split(payload.unsafe_dbt_cli_command)

    output = DbtManager.execute_unsafe_dbt_command(shlex_list_cli)

    metadata = {"dbt_command": payload.unsafe_dbt_cli_command}

    return DbtUnsafeResponse(status="success", output=output, metadata=metadata)
