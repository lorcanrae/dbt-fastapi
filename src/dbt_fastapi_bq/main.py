from fastapi import FastAPI

from dbt_fastapi_bq.routes import dbt_router


app = FastAPI(
    title="dbt FastAPI Wrapper",
    version="0.3.0",
    description="Exposes dbt CLI over HTTP",
)


@app.get("/")
def root():
    return {"status": "running", "Author": "Lorcan Rae"}


app.include_router(dbt_router)
