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
    summary="Execute arbitrary dbt command. Requiers the entire dbt CLI command to be executed. No error handling.",
)
async def run_dbt(
    payload: DbtUnsafeRequest,
) -> DbtCommandResponse:
    try:
        result = subprocess.run(
            shlex.split(payload.unsafe_dbt_cli_command), capture_output=True, text=True
        )
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
