from fastapi import APIRouter

from dbt_fastapi.dbt_manager import DbtManager

from dbt_fastapi.schemas.dbt_schema import (
    DbtRunTestCompileSeedSnapshotDocs,
    DbtCommandResponse,
)


router = APIRouter()

COMMAND = "test"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}'",
)
async def run_dbt(
    payload: DbtRunTestCompileSeedSnapshotDocs,
) -> DbtCommandResponse:
    dbt_manager = DbtManager(verb=COMMAND, **payload.model_dump())

    # Execute dbt command
    result = dbt_manager.execute_dbt_command()

    # Extract list of nodes found
    nodes = dbt_manager.get_nodes_from_result(result)

    output_str = f"dbt {COMMAND} completed with success={result.success}"

    if hasattr(result, "result") and result.result:
        if hasattr(result.result, "results"):
            output_str += f", {len(result.result.results)} tests processed"

    metadata = {
        "dbt_command": " ".join(dbt_manager.dbt_cli_args),
        "success": result.success,
        "tests_processed": len(nodes),
        "selection_criteria": dbt_manager._get_selection_criteria_string(),
    }

    return DbtCommandResponse(
        status="success", output=output_str, nodes=nodes, metadata=metadata
    )
