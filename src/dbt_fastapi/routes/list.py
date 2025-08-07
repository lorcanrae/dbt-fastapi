from fastapi import APIRouter

from dbt_fastapi.dbt_manager import DbtManager
from dbt_fastapi.schemas.dbt_schema import (
    DbtBuildListRequest,
    DbtListResponse,
)

router = APIRouter()

COMMAND = "list"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}' to get filtered list of nodes",
    response_model=DbtListResponse,
)
async def list_dbt_nodes(
    payload: DbtBuildListRequest,
) -> DbtListResponse:
    """
    List dbt nodes based on selection criteria.

    Returns a structured list of nodes that match the provided selection,
    exclusion, and selector arguments.
    """
    dbt_manager = DbtManager(verb=COMMAND, **payload.model_dump())

    # Execute the dbt command
    result = dbt_manager.execute_dbt_command()

    # Extract list of nodes found
    nodes = dbt_manager.get_list_nodes(result)

    metadata = {
        "dbt_command": " ".join(dbt_manager.dbt_cli_args),
        "total_nodes": len(nodes),
        "selection_criteria": dbt_manager._get_selection_criteria_string(),
    }

    return DbtListResponse(
        status="success",
        nodes=nodes,
        metadata=metadata,
    )
