from fastapi import APIRouter

from dbt_fastapi_bq.dbt_manager import DbtManager

from dbt_fastapi_bq.schemas.dbt_schema import (
    DbtRunTestRequest,
    DbtCommandResponse,
)


router = APIRouter()

COMMAND = "test"


@router.post(
    f"/{COMMAND}",
    summary=f"Execute 'dbt {COMMAND}'",
)
async def run_dbt(
    payload: DbtRunTestRequest,
) -> DbtCommandResponse:
    dbt_manager = DbtManager(verb=COMMAND, **payload.model_dump())

    from pprint import pprint as print

    print(dbt_manager.__dict__)

    output = dbt_manager.execute_dbt_command()
    metadata = {"dbt_command": " ".join(dbt_manager.dbt_cmd)}

    return DbtCommandResponse(status="success", output=output, metadata=metadata)
