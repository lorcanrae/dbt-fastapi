from fastapi import FastAPI
from dbt_fastapi_bq.api import dbt

app = FastAPI(
    title="dbt FastAPI Wrapper",
    version="0.2.0",
    description="Exposes dbt CLI over HTTP",
)

app.include_router(dbt.router, prefix="/dbt", tags=["dbt"])


@app.get("/")
def root():
    return {"status": "running"}
