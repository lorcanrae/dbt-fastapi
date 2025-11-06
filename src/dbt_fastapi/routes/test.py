from fastapi import APIRouter, Depends, HTTPException, status

from dbt_fastapi.dbt_manager import DbtManager
from dbt_fastapi.config import DbtConfig, get_dbt_config
from dbt_fastapi.schemas.request_schema import DbtTestRequest
from dbt_fastapi.schemas.response_schema import DbtTestResponse, DbtTestBuildMetadata


router = APIRouter()


COMMAND = "test"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}'",
    response_model=DbtTestResponse,
)
def run_dbt(
    payload: DbtTestRequest,
    config: DbtConfig = Depends(get_dbt_config),
) -> DbtTestResponse:
    """
    Execute dbt test command.

    Returns information about the tests that were executed, including:
    - Which tests passed/failed
    - Test execution details
    - Summary statistics

    Note: Endpoint returns 200 even when tests fail.
    Check 'success' field and 'test_summary' for actual test results.
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

    # Extract test summary
    test_summary = dbt_manager.get_test_summary(result)

    metadata = DbtTestBuildMetadata(
        command=COMMAND,
        dbt_command=" ".join(dbt_manager.dbt_cli_args),
        target=dbt_manager.target,
        nodes_processed=len(nodes),
        selection_criteria=dbt_manager.get_selection_criteria_string(),
        has_test_failures=test_summary.get("failed", 0) > 0,
        has_test_errors=test_summary.get("errored", 0) > 0,
    )

    if payload.fail_on_test_failures and (
        metadata.has_test_failures or metadata.has_test_error
    ):
        failed_tests = []
        passed_tests = []
        for node in nodes:
            if node.test_result and node.test_result.status.value in ["fail", "error"]:
                failed_tests.append(
                    {
                        "unique_id": node.test_result.unique_id,
                        "name": node.test_result.name,
                        "status": node.test_result.message,
                        "failures": node.test_result.failures,
                        "execution_time": node.test_result.execution_time,
                    }
                )
            else:
                passed_tests.append(
                    {
                        "unique_id": node.test_result.unique_id,
                        "name": node.test_result.name,
                        "status": node.test_result.message,
                        "failures": None,
                        "execution_time": node.test_result.execution_time,
                    }
                )
        error_parts = []
        if metadata.has_test_failures:
            error_parts.append(f"{test_summary['failed']} test(s) failed")
        if metadata.has_test_errors:
            error_parts.append(f"{test_summary['errored']} test(s) errored")

        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "TestExecutionFailed",
                "message": ", ".join(error_parts),
                "test_summary": test_summary,
                "failed_tests": failed_tests,
                "passed_tests": passed_tests,
                "metadata": {
                    "command": COMMAND,
                    "target": dbt_manager.target,
                    "selection_criteria": dbt_manager.get_selection_criteria_string(),
                },
            },
        )

    return DbtTestResponse(
        success=result.success,
        nodes=nodes,
        metadata=metadata,
        test_summary=test_summary,
    )
