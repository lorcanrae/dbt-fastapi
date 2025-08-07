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

    result = dbt_manager.execute_dbt_command()

    output_str = f"dbt {COMMAND} completed with success={result.success}"

    if hasattr(result, "result") and result.result:
        output_str += f", {len(result.result.results)} models processed"

    metadata = {
        "dbt_command": " ".join(dbt_manager.dbt_cli_args),
        "success": result.success,
        "models_processed": len(result.result.results)
        if hasattr(result, "result") and result.result
        else 0,
    }

    return DbtCommandResponse(status="success", output=output_str, metadata=metadata)
