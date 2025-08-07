from fastapi import APIRouter

from dbt_fastapi.dbt_manager import DbtManager

from dbt_fastapi.schemas.dbt_schema import (
    DbtBuildListRequest,
    DbtCommandResponse,
)


router = APIRouter()

COMMAND = "build"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}'",
)
async def run_dbt(
    payload: DbtBuildListRequest,
) -> DbtCommandResponse:
    dbt_manager = DbtManager(verb=COMMAND, **payload.model_dump())

    output = dbt_manager.execute_dbt_command()
    metadata = {"dbt_command": " ".join(dbt_manager.dbt_cli_args)}

    return DbtCommandResponse(status="success", output=output, metadata=metadata)
