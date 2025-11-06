from fastapi import APIRouter, Depends

from dbt_fastapi.dbt_manager import DbtManager
from dbt_fastapi.config import DbtConfig, get_dbt_config
from dbt_fastapi.schemas.request_schema import DbtCompileRequest
from dbt_fastapi.schemas.response_schema import DbtCompileResponse, DbtMetadataBase


router = APIRouter()


COMMAND = "compile"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}'",
    response_model=DbtCompileResponse,
)
def run_dbt(
    payload: DbtCompileRequest,
    config: DbtConfig = Depends(get_dbt_config),
) -> DbtCompileResponse:
    """
    Execute dbt compile command.

    Returns information about the nodes that were compiled.
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

    metadata = DbtMetadataBase(
        command=COMMAND,
        dbt_command=" ".join(dbt_manager.dbt_cli_args),
        target=dbt_manager.target,
        nodes_processed=len(nodes),
        selection_criteria=dbt_manager.get_selection_criteria_string(),
    )

    return DbtCompileResponse(
        success=result.success,
        nodes=nodes,
        metadata=metadata,
    )
