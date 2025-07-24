from fastapi import APIRouter
from . import run

router = APIRouter()

router.include_router(run.router)
