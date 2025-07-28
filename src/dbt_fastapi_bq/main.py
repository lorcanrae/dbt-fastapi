from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


from dbt_fastapi_bq.routes import api_router
from dbt_fastapi_bq.exceptions import DbtCommandException

app = FastAPI(
    title="dbt FastAPI Wrapper",
    version="0.2.0",
    description="Exposes dbt CLI over HTTP",
)


@app.get("/")
def root():
    return {"status": "running"}


app.include_router(api_router)


@app.exception_handler(DbtCommandException)
async def dbt_run_exception_handler(request: Request, exc: DbtCommandException):
    validated = exc.detail.model_dump()
    return JSONResponse(
        status_code=exc.status_code,
        content=validated,
    )
