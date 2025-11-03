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
def run_dbt(
    payload: DbtRunTestCompileSeedSnapshotDocsRequest,
) -> DbtResponse:
    """
    Execute dbt test command.

    Returns information about the tests that were executed, including:
    - Which tests passed/failed
    - Test execution details
    - Summary statistics

    Note: Endpoint returns 200 even when tests fail.
    Check 'success' field and 'test_summary' for actual test results.
    """
    dbt_manager = DbtManager(verb=COMMAND, **payload.model_dump())

    # Execute dbt command
    result = dbt_manager.execute_dbt_command()

    # Extract list of nodes
    nodes = dbt_manager.get_nodes_from_result(result)

    # Extract test summary
    test_summary = dbt_manager.get_test_summary(result)

    print(test_summary)

    metadata = {
        "command": COMMAND,
        "dbt_command": " ".join(dbt_manager.dbt_cli_args),
        "target": dbt_manager.target,
        "nodes_processed": len(nodes),
        "selection_criteria": dbt_manager._get_selection_criteria_string(),
        "has_test_failures": test_summary.get("failed", 0) > 0
        if test_summary
        else False,
        "has_test_errors": test_summary.get("errored", 0) > 0
        if test_summary
        else False,
    }

    return DbtResponse(
        success=result.success,
        nodes=nodes,
        metadata=metadata,
        test_summary=test_summary,
    )
