from fastapi import APIRouter
from . import dbt

api_router = APIRouter()
api_router.include_router(dbt.router)
