from fastapi import APIRouter, HTTPException, status
import subprocess
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
    shlex_lst = shlex.split(payload.unsafe_dbt_cli_command)
    if shlex_lst[0] != "dbt":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Not a dbt command",
                "attempted_command": payload.unsafe_dbt_cli_command,
            },
        )

    try:
        result = subprocess.run(shlex_lst, capture_output=True, text=True)
        output = DbtManager.strip_ansi_codes(result.stdout)
    except subprocess.CalledProcessError as e:
        output = (
            DbtManager.strip_ansi_codes(e.stderr or e.stdout)
            or "No error message captured."
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "dbt command failed",
                "message": output,
            },
        )

    metadata = {"dbt_command": payload.unsafe_dbt_cli_command}

    return DbtCommandResponse(status="success", output=output, metadata=metadata)
