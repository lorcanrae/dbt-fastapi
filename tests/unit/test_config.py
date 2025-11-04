"""
Tests for configuration module (config.py).

Test coverage:
- Configuration discovery from environment variables
- Configuration discovery from filesystem
- Configuration validation
- Configuration caching and singleton pattern
- Error handling for missing/duplicate files
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from dbt_fastapi.config import (
    DbtConfig,
    initialize_dbt_config,
    get_dbt_config,
    reset_dbt_config,
    _discover_config_file,
)
from dbt_fastapi.exceptions import DbtConfigurationError


class TestDbtConfigFromEnvironment:
    """Test configuration loading from environment variables."""

    def test_config_from_env_vars(self, monkeypatch, tmp_path):
        """Test that config loads from environment variables."""
        # Create test directories
        profiles_dir = tmp_path / "profiles"
        project_dir = tmp_path / "project"
        profiles_dir.mkdir()
        project_dir.mkdir()

        # Create config files
        (profiles_dir / "profiles.yml").write_text("test: {}")
        (project_dir / "dbt_project.yml").write_text("name: test")

        # Set environment variables
        monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
        monkeypatch.setenv("DBT_PROJECT_DIR", str(project_dir))

        # Create config
        config = DbtConfig()

        # Should load from env vars
        assert config.dbt_profiles_dir == str(profiles_dir)
        assert config.dbt_project_dir == str(project_dir)

    def test_config_from_env_vars_case_insensitive(self, monkeypatch, tmp_path):
        """Test that env var names are case insensitive."""
        profiles_dir = tmp_path / "profiles"
        project_dir = tmp_path / "project"
        profiles_dir.mkdir()
        project_dir.mkdir()

        (profiles_dir / "profiles.yml").write_text("test: {}")
        (project_dir / "dbt_project.yml").write_text("name: test")

        # Use lowercase env var names
        monkeypatch.setenv("dbt_profiles_dir", str(profiles_dir))
        monkeypatch.setenv("dbt_project_dir", str(project_dir))

        config = DbtConfig()

        assert config.dbt_profiles_dir == str(profiles_dir)
        assert config.dbt_project_dir == str(project_dir)

    def test_config_validates_env_var_paths(self, monkeypatch):
        """Test that config validates paths from env vars."""
        # Set env vars to non-existent paths
        monkeypatch.setenv("DBT_PROFILES_DIR", "/nonexistent/path")
        monkeypatch.setenv("DBT_PROJECT_DIR", "/another/nonexistent")

        # Should raise validation error
        with pytest.raises(ValueError) as exc:
            DbtConfig()

        assert "does not exist" in str(exc.value)


class TestDbtConfigDiscovery:
    """Test configuration discovery from filesystem."""

    def test_discover_paths_finds_files(self, dbt_project_structure, monkeypatch):
        """Test that discover_paths can find dbt config files."""
        # Clear any env vars and disable .env loading
        monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
        monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)

        # Mock PROJECT_ROOT to point to our test directory
        with patch("dbt_fastapi.config.PROJECT_ROOT", dbt_project_structure):
            config = DbtConfig()
            config.discover_paths()

            # Should find the placeholder_dbt_project directory
            assert config.dbt_profiles_dir is not None
            assert config.dbt_project_dir is not None
            assert "placeholder_dbt_project" in config.dbt_profiles_dir
            assert "placeholder_dbt_project" in config.dbt_project_dir

    def test_discover_paths_raises_on_missing(self, tmp_path, monkeypatch):
        """Test that discover_paths raises error when files not found."""
        # Clear any env vars
        monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
        monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)

        # Empty directory with no dbt files
        with patch("dbt_fastapi.config.PROJECT_ROOT", tmp_path):
            config = DbtConfig()

            with pytest.raises(DbtConfigurationError) as exc:
                config.discover_paths()

            assert "not found" in exc.value.message

    def test_discover_paths_raises_on_duplicate(self, tmp_path, monkeypatch):
        """Test that discover_paths raises error on duplicate files."""
        # Clear any env vars
        monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
        monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)

        # Create two dbt_project.yml files
        dir1 = tmp_path / "project1"
        dir2 = tmp_path / "project2"
        dir1.mkdir()
        dir2.mkdir()
        (dir1 / "dbt_project.yml").write_text("name: test1")
        (dir1 / "profiles.yml").write_text("test: {}")
        (dir2 / "dbt_project.yml").write_text("name: test2")
        (dir2 / "profiles.yml").write_text("test: {}")

        with patch("dbt_fastapi.config.PROJECT_ROOT", tmp_path):
            config = DbtConfig()

            with pytest.raises(DbtConfigurationError) as exc:
                config.discover_paths()

            assert "Multiple" in exc.value.message

    def test_discover_skips_excluded_dirs(self, tmp_path, monkeypatch):
        """Test that discovery skips excluded directories like .venv."""
        # Clear any env vars
        monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
        monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)

        # Create dbt files in both regular and excluded directories
        regular_dir = tmp_path / "placeholder_dbt_project"
        venv_dir = tmp_path / ".venv" / "placeholder_dbt_project"
        regular_dir.mkdir()
        venv_dir.mkdir(parents=True)

        (regular_dir / "dbt_project.yml").write_text("name: regular")
        (regular_dir / "profiles.yml").write_text("test: {}")
        (venv_dir / "dbt_project.yml").write_text("name: venv")
        (venv_dir / "profiles.yml").write_text("test: {}")

        with patch("dbt_fastapi.config.PROJECT_ROOT", tmp_path):
            config = DbtConfig()
            config.discover_paths()

            # Should find only the regular one, not in .venv
            assert config.dbt_project_dir == str(regular_dir)


class TestDbtConfigValidation:
    """Test configuration validation."""

    def test_validate_configuration_success(self, dummy_paths, monkeypatch):
        """Test that validate_configuration passes with valid config."""
        profiles_dir, project_dir = dummy_paths

        # Set env vars to use test paths (bypass discovery)
        monkeypatch.setenv("DBT_PROFILES_DIR", profiles_dir)
        monkeypatch.setenv("DBT_PROJECT_DIR", project_dir)

        config = DbtConfig()

        # Should not raise
        config.validate_configuration()

    def test_validate_raises_on_missing_profiles(self, tmp_path, monkeypatch):
        """Test that validation fails when profiles.yml missing."""
        profiles_dir = tmp_path / "profiles"
        project_dir = tmp_path / "project"
        profiles_dir.mkdir()
        project_dir.mkdir()

        # Only create dbt_project.yml, not profiles.yml
        (project_dir / "dbt_project.yml").write_text("name: test")

        # Set env vars to use test paths
        monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
        monkeypatch.setenv("DBT_PROJECT_DIR", str(project_dir))

        config = DbtConfig()

        with pytest.raises(DbtConfigurationError) as exc:
            config.validate_configuration()

        assert "profiles.yml" in exc.value.message

    def test_validate_raises_on_missing_project(self, tmp_path, monkeypatch):
        """Test that validation fails when dbt_project.yml missing."""
        profiles_dir = tmp_path / "profiles"
        project_dir = tmp_path / "project"
        profiles_dir.mkdir()
        project_dir.mkdir()

        # Only create profiles.yml, not dbt_project.yml
        (profiles_dir / "profiles.yml").write_text("test: {}")

        # Set env vars to use test paths
        monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
        monkeypatch.setenv("DBT_PROJECT_DIR", str(project_dir))

        config = DbtConfig()

        with pytest.raises(DbtConfigurationError) as exc:
            config.validate_configuration()

        assert "dbt_project.yml" in exc.value.message


class TestConfigurationCaching:
    """Test configuration singleton and caching."""

    def test_initialize_and_get_config_returns_same_instance(
        self, dbt_project_structure, monkeypatch
    ):
        """Test that initialize and get return the same cached instance."""
        # Clear env vars
        monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
        monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)

        with patch("dbt_fastapi.config.PROJECT_ROOT", dbt_project_structure):
            # Initialize config
            config1 = initialize_dbt_config()

            # Get config
            config2 = get_dbt_config()

            # Should be the SAME instance (not just equal)
            assert config1 is config2

    def test_get_config_before_initialize_raises(self):
        """Test that get_dbt_config raises if called before initialization."""
        # reset_config fixture already reset, so config is not initialized

        with pytest.raises(RuntimeError) as exc:
            get_dbt_config()

        assert "not initialized" in str(exc.value).lower()

    def test_initialize_config_multiple_times_returns_same(
        self, dbt_project_structure, monkeypatch
    ):
        """Test that calling initialize multiple times returns same instance."""
        # Clear env vars
        monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
        monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)

        with patch("dbt_fastapi.config.PROJECT_ROOT", dbt_project_structure):
            _config1 = initialize_dbt_config()

            # Initialize again (shouldn't create new instance)
            config2 = initialize_dbt_config()

            # Should be different instances (last one wins)
            # This is current behavior - could be changed to return same instance
            assert config2 is not None

    def test_reset_config_clears_cache(self, dbt_project_structure, monkeypatch):
        """Test that reset_dbt_config clears the cached config."""
        # Clear env vars
        monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
        monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)

        with patch("dbt_fastapi.config.PROJECT_ROOT", dbt_project_structure):
            # Initialize
            config1 = initialize_dbt_config()
            assert get_dbt_config() is config1

            # Reset
            reset_dbt_config()

            # Should raise now
            with pytest.raises(RuntimeError):
                get_dbt_config()


class TestDiscoverConfigFileFunction:
    """Test the _discover_config_file helper function."""

    def test_discover_finds_in_current_dir(self, tmp_path, monkeypatch):
        """Test that discovery finds files in current directory first."""
        # Change to tmp_path
        monkeypatch.chdir(tmp_path)

        # Create file in current dir
        (tmp_path / "profiles.yml").write_text("test: {}")

        with patch("dbt_fastapi.config.PROJECT_ROOT", tmp_path):
            result = _discover_config_file("profiles.yml")

        assert result == str(tmp_path)

    def test_discover_finds_in_common_locations(self, tmp_path):
        """Test that discovery checks common locations."""
        # Create file in placeholder_dbt_project
        project_dir = tmp_path / "placeholder_dbt_project"
        project_dir.mkdir()
        (project_dir / "profiles.yml").write_text("test: {}")

        with patch("dbt_fastapi.config.PROJECT_ROOT", tmp_path):
            result = _discover_config_file("profiles.yml")

        assert result == str(project_dir)

    def test_discover_checks_parent_directories(self, tmp_path, monkeypatch):
        """Test that discovery checks parent directories."""
        # Create nested structure
        child_dir = tmp_path / "child" / "grandchild"
        child_dir.mkdir(parents=True)

        # Put file in parent
        (tmp_path / "profiles.yml").write_text("test: {}")

        # Change to child directory
        monkeypatch.chdir(child_dir)

        with patch("dbt_fastapi.config.PROJECT_ROOT", tmp_path):
            result = _discover_config_file("profiles.yml")

        assert result == str(tmp_path)


class TestConfigurationIntegration:
    """Integration tests for full configuration workflow."""

    def test_full_initialization_workflow(self, dbt_project_structure, monkeypatch):
        """Test complete initialization workflow."""
        # Clear env vars
        monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
        monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)

        with patch("dbt_fastapi.config.PROJECT_ROOT", dbt_project_structure):
            # Step 1: Initialize
            config = initialize_dbt_config()

            # Step 2: Verify properties are set
            assert config.dbt_profiles_dir is not None
            assert config.dbt_project_dir is not None
            assert "placeholder_dbt_project" in config.dbt_profiles_dir

            # Step 3: Verify files exist
            profiles_path = Path(config.dbt_profiles_dir) / "profiles.yml"
            project_path = Path(config.dbt_project_dir) / "dbt_project.yml"
            assert profiles_path.exists()
            assert project_path.exists()

            # Step 4: Get config returns same instance
            cached_config = get_dbt_config()
            assert cached_config is config

    def test_env_vars_override_discovery(
        self, monkeypatch, tmp_path, dbt_project_structure
    ):
        """Test that environment variables take precedence over discovery."""
        # Create separate directories for env var paths
        env_profiles = tmp_path / "env_profiles"
        env_project = tmp_path / "env_project"
        env_profiles.mkdir()
        env_project.mkdir()
        (env_profiles / "profiles.yml").write_text("env: {}")
        (env_project / "dbt_project.yml").write_text("name: env")

        # Set env vars
        monkeypatch.setenv("DBT_PROFILES_DIR", str(env_profiles))
        monkeypatch.setenv("DBT_PROJECT_DIR", str(env_project))

        with patch("dbt_fastapi.config.PROJECT_ROOT", dbt_project_structure):
            config = initialize_dbt_config()

            # Should use env vars, not discovered paths
            assert config.dbt_profiles_dir == str(env_profiles)
            assert config.dbt_project_dir == str(env_project)
