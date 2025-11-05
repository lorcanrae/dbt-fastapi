from enum import Enum


# === Enums ===


class ResponseTestStatus(str, Enum):
    """Enum for test execution status."""

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    ERROR = "error"
    SKIP = "skip"


class DbtResourceType(str, Enum):
    """Valid dbt resouce types for filtering"""

    MODEL = "model"
    TEST = "test"
    SNAPSHOT = "snapshot"
    SEED = "seed"
    SOURCE = "source"
    EXPOSURE = "exposure"
    METRIC = "metric"
    ANALYSIS = "analysis"
