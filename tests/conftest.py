import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from dbt_fastapi.main import app


# === API Fixture ===


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


# === DbtManager Fixtures ===


@pytest.fixture
def dummy_paths(tmp_path: Path) -> tuple[str, str]:
    """
    Create temporary directories with fake dbt_project.yml and profiles.yml
    """
    dbt_dir = tmp_path / "dbt_project"
    profiles_dir = tmp_path / "profiles"

    dbt_dir.mkdir()
    profiles_dir.mkdir()

    (dbt_dir / "dbt_project.yml").write_text("name: testing")
    (dbt_dir / "dbt_project.yml").write_text("default: testing")

    return str(profiles_dir), str(dbt_dir)
