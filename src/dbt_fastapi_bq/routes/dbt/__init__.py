from fastapi import APIRouter
from . import run, test, build

router = APIRouter(prefix="/dbt", tags=["dbt"])

router.include_router(run.router)
router.include_router(test.router)
router.include_router(build.router)
