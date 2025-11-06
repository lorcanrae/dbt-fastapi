from fastapi import APIRouter
from . import run, test, build, list, compile

dbt_router = APIRouter(prefix="/dbt", tags=["dbt"])

dbt_router.include_router(run.router)
dbt_router.include_router(test.router)
dbt_router.include_router(build.router)
dbt_router.include_router(list.router)
dbt_router.include_router(compile.router)
