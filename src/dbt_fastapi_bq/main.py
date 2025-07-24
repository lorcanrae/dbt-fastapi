from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


from dbt_fastapi_bq.routes import dbt
from dbt_fastapi_bq.exceptions import DbtRunException

app = FastAPI(
    title="dbt FastAPI Wrapper",
    version="0.2.0",
    description="Exposes dbt CLI over HTTP",
)


@app.get("/")
def root():
    return {"status": "running"}


app.include_router(dbt.router, prefix="/dbt", tags=["dbt"])


@app.exception_handler(DbtRunException)
async def dbt_run_exception_handler(request: Request, exc: DbtRunException):
    validated = exc.detail.model_dump()
    return JSONResponse(
        status_code=exc.status_code,
        content=validated,
    )
