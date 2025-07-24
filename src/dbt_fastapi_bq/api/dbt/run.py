from fastapi import APIRouter, Depends, HTTPException, status
import subprocess
import os

from dbt_fastapi_bq.models.dbt_models import DbtRunRequest, DbtRunResult
from dbt_fastapi_bq.utils.payload_validators import validate_dbt_run_request

router = APIRouter()


@router.post("/run", response_model=DbtRunResult)
async def run_dbt(
    payload: DbtRunRequest = Depends(validate_dbt_run_request),
) -> dict[str, str]:
    command: list[str] = ["dbt", "run"]

    if payload.model:
        select_arg = payload.model
        if payload.upstream:
            select_arg = f"+{select_arg}"
        if payload.downstream:
            select_arg = f"{select_arg}+"
        command += ["--select", select_arg]

    command += ["--target", payload.target]

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"dbt run failed: {e}",
        )

    # try:
    #     output = run_dbt_command(
    #         [
    #             "dbt",
    #             "run",
    #             "--project-dir",
    #             "dbt_project",
    #             "--profiles-dir",
    #             "dbt_project",
    #         ]
    #     )
    #     return DbtRunResult(success=True, output=output)
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))


@router.get("/runtest")
def run_test():
    output = os.getenv("DBT_TARGET", "not found")
    return {"output": output}
