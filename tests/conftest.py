"""
Test configuration and fixtures.

Key fixtures:
- client: FastAPI test client
- reset_config: Automatically reset configuration between tests
- dummy_paths: Create temporary dbt project directories
"""

import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from dbt_fastapi.config import reset_dbt_config
from dbt_fastapi import config


# ============================================================================
# Configuration Reset Fixture
# ============================================================================


@pytest.fixture(autouse=True)
def reset_config(monkeypatch, tmp_path):
    """
    Automatically reset configuration between tests.

    This ensures tests don't interfere with each other by:
    1. Clearing environment variables that affect DbtConfig
    2. Disabling .env file loading
    3. Resetting config cache before and after each test

    The autouse=True means this runs for EVERY test automatically.
    """
    # Clear environment variables that DbtConfig might load
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    monkeypatch.delenv("dbt_profiles_dir", raising=False)
    monkeypatch.delenv("dbt_project_dir", raising=False)

    # Patch environment variable
    monkeypatch.setenv("DBT_PROJECT_NAME", "placeholder_dbt_project")

    # Patch Path.cwd() to something harmless
    def fake_cwd():
        return tmp_path

    monkeypatch.setattr(Path, "cwd", fake_cwd)

    # Patch__file__ in the config module to make it's parent dirs resolve to tmp_path
    fake_module_file = tmp_path / "config" / "config.py"
    fake_module_file.parent.mkdir()
    fake_module_file.write_text("#fake config module")
    monkeypatch.setattr(config, "__file__", str(fake_module_file))

    # Disable .env file loading for tests by setting env_file to None
    # This prevents DbtConfig from loading from the real .env file
    from dbt_fastapi.config import DbtConfig

    original_config = DbtConfig.model_config.copy()
    DbtConfig.model_config["env_file"] = None

    reset_dbt_config()  # Reset before test
    yield
    reset_dbt_config()  # Reset after test

    # Restore original config
    DbtConfig.model_config.update(original_config)


# ============================================================================
# FastAPI Test Client Fixture
# ============================================================================


@pytest.fixture(scope="module")
def client() -> TestClient:
    """
    FastAPI test client for integration tests.

    Note: This is module-scoped, so the client is created once per test module.
    The reset_config fixture ensures configuration is reset between tests.
    """
    # Import here to avoid issues with lifespan
    from dbt_fastapi.main import app

    return TestClient(app)


# ============================================================================
# Dummy Path Fixtures
# ============================================================================


@pytest.fixture
def dummy_paths(tmp_path: Path) -> tuple[str, str]:
    """
    Create temporary directories with fake dbt configuration files.

    Returns:
        Tuple of (profiles_dir, project_dir) as strings

    Example:
        def test_something(dummy_paths):
            profiles_dir, project_dir = dummy_paths
            manager = DbtManager(
                verb="run",
                target="dev",
                profiles_dir=profiles_dir,
                project_dir=project_dir,
            )
    """
    # Create directories
    profiles_dir = tmp_path / "profiles"
    project_dir = tmp_path / "project"
    profiles_dir.mkdir()
    project_dir.mkdir()

    # Create fake config files
    (profiles_dir / "profiles.yml").write_text("""
default:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: test-project
      dataset: test_dataset
    prod:
      type: bigquery
      method: service-account
      project: test-project
      dataset: prod_dataset
  target: dev
""")

    (project_dir / "dbt_project.yml").write_text("""
name: test_project
version: 1.0.0
profile: default
""")

    return str(profiles_dir), str(project_dir)


@pytest.fixture
def dbt_project_structure(tmp_path: Path) -> Path:
    """
    Create a complete dbt project structure for testing configuration discovery.

    Returns:
        Path to the root directory containing the dbt project

    Structure:
        tmp_path/
        └── placeholder_dbt_project/
            ├── dbt_project.yml
            └── profiles.yml

    Example:
        def test_discovery(dbt_project_structure):
            with patch("dbt_fastapi.config.PROJECT_ROOT", dbt_project_structure):
                config = DbtConfig()
                config.discover_paths()
                assert config.dbt_project_dir is not None
    """
    # Create dbt project directory
    project_dir = tmp_path / "placeholder_dbt_project"
    project_dir.mkdir()

    # Create config files
    (project_dir / "dbt_project.yml").write_text("""
name: test_project
version: 1.0.0
profile: default
""")

    (project_dir / "profiles.yml").write_text("""
default:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: test-project
      dataset: test_dataset
  target: dev
""")

    return tmp_path


# ============================================================================
# Mock Fixtures
# ============================================================================


@pytest.fixture
def mock_dbt_result():
    """
    Create a mock dbtRunnerResult for testing.

    Returns a successful result with empty results list.
    """
    from unittest.mock import Mock

    mock_result = Mock()
    mock_result.success = True
    mock_result.exception = None
    mock_result.result = Mock()
    mock_result.result.results = []

    return mock_result


@pytest.fixture
def mock_failed_dbt_result():
    """
    Create a mock failed dbtRunnerResult for testing.
    """
    from unittest.mock import Mock

    mock_result = Mock()
    mock_result.success = False
    mock_result.exception = None
    mock_result.result = Mock()
    mock_result.result.results = []

    return mock_result
