from fastapi import APIRouter, Depends

from dbt_fastapi.dbt_manager import DbtManager
from dbt_fastapi.config import DbtConfig, get_dbt_config
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
def list_dbt_nodes(
    payload: DbtBuildListRequest,
    config: DbtConfig = Depends(get_dbt_config),
) -> DbtResponse:
    """
    List dbt nodes based on selection criteria.

    Returns a list of nodes that match the provided selection,
    exclusion, and selector arguments.
    """
    dbt_manager = DbtManager(
        verb=COMMAND,
        target=payload.target or config.dbt_target_default,
        profiles_dir=config.dbt_profiles_dir,
        project_dir=config.dbt_project_dir,
        select_args=payload.select_args,
        exclude_args=payload.exclude_args,
        selector_args=payload.selector_args,
    )
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
