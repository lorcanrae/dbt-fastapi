from fastapi import APIRouter

from dbt_fastapi_bq.schemas.dbt_command import (
    DbtCommandRequest,
    DbtCommandResponse,
)
from dbt_fastapi_bq.utils import (
    execute_dbt_command,
    generate_dbt_command,
)


router = APIRouter()

COMMAND = "test"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}'",
)
async def run_dbt(
    payload: DbtCommandRequest,
) -> DbtCommandResponse:
    dbt_cmd = generate_dbt_command(COMMAND, payload)
    stdout: str = execute_dbt_command(dbt_cmd, payload=payload)

    return DbtCommandResponse(status="success", output=stdout, metadata={})
