from fastapi import APIRouter

from dbt_fastapi.dbt_manager import DbtManager
from dbt_fastapi.schemas.dbt_schema import (
    DbtBuildListRequest,
    DbtResponse,
)

router = APIRouter()

COMMAND = "list"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}' to get filtered list of nodes",
    response_model=DbtResponse,
)
async def list_dbt_nodes(
    payload: DbtBuildListRequest,
) -> DbtResponse:
    """
    List dbt nodes based on selection criteria.

    Returns a list of nodes that match the provided selection,
    exclusion, and selector arguments.
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
