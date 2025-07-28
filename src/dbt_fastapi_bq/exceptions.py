from dbt_fastapi_bq.models.dbt_models import BaseDbtError


class DbtCommandException(Exception):
    def __init__(self, detail: BaseDbtError, status_code: int):
        self.detail = detail
        self.status_code = status_code
