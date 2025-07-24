from dbt_fastapi_bq.models.dbt_models import DbtRunRequest
from fastapi import HTTPException


def validate_dbt_run_request(payload: DbtRunRequest) -> DbtRunRequest:
    if (payload.upstream or payload.downstream) and not payload.model:
        raise HTTPException(
            status_code=400,
            detail="`model` must be provided if select_prefix or select_suffix is True.",
        )
    return payload
