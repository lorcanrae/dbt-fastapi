from fastapi import FastAPI

from dbt_fastapi.routes import dbt_router
from dbt_fastapi.exceptions import DbtFastApiError
from dbt_fastapi.exception_handlers import dbt_error_handler, generic_exception_handler


app = FastAPI(
    title="dbt FastAPI Wrapper",
    version="0.3.0",
    description="dbt over HTTP",
)

app.add_exception_handler(DbtFastApiError, dbt_error_handler)
app.add_exception_handler(Exception, generic_exception_handler)


@app.get("/")
def root():
    return {"status": "running", "Author": "Lorcan Rae"}


app.include_router(dbt_router)
