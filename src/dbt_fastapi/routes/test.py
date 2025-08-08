from fastapi import APIRouter

from dbt_fastapi.dbt_manager import DbtManager
from dbt_fastapi.schemas.dbt_schema import (
    DbtRunTestCompileSeedSnapshotDocsRequest,
    DbtResponse,
)


router = APIRouter()

COMMAND = "test"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}'",
    response_model=DbtResponse,
)
async def run_dbt(
    payload: DbtRunTestCompileSeedSnapshotDocsRequest,
) -> DbtResponse:
    """
    Execute dbt test command.

    Returns information about the tests that were executed.
    """
    dbt_manager = DbtManager(verb=COMMAND, **payload.model_dump())

    # Execute dbt command
    result = dbt_manager.execute_dbt_command()

    # Extract list of nodes
    nodes = dbt_manager.get_nodes_from_result(result)

    metadata = {
        "command": COMMAND,
        "dbt_command": " ".join(dbt_manager.dbt_cli_args),
        "target": dbt_manager.target,
        "nodes_processed": len(nodes),
        "selection_criteria": dbt_manager._get_selection_criteria_string(),
    }

    return DbtResponse(
        success=result.success,
        nodes=nodes,
        metadata=metadata,
    )
