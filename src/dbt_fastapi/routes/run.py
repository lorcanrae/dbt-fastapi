from fastapi import APIRouter

from dbt_fastapi.dbt_manager import DbtManager

from dbt_fastapi.schemas.dbt_schema import (
    DbtRunTestCompileSeedSnapshotDocs,
    DbtCommandResponse,
)


router = APIRouter()

COMMAND = "run"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}'",
)
async def run_dbt(
    payload: DbtRunTestCompileSeedSnapshotDocs,
) -> DbtCommandResponse:
    dbt_manager = DbtManager(verb=COMMAND, **payload.model_dump())

    print(dbt_manager.dbt_cli_command)

    output = dbt_manager.execute_dbt_command()
    metadata = {"dbt_command": " ".join(dbt_manager.dbt_cli_command)}

    return DbtCommandResponse(status="success", output=output, metadata=metadata)
