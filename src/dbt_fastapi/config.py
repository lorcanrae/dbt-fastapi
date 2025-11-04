"""
Configuration management for dbt-fastapi.

This module handles discovery and caching of dbt configuration paths.
Configuration is discovered once at application startup and cached for reuse
across all requests.

Design Principles:
- Discover configuration once at startup (not on every request)
- Support environment variables for containerized deployments
- Fail fast with clear error messages if configuration is invalid
- Thread-safe for use with FastAPI's thread pool
- Testable with dependency injection
"""

import os
import logging
from pathlib import Path
from typing import Optional
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from dbt_fastapi.exceptions import (
    create_configuration_missing_error,
    create_configuration_duplicate_error,
)


logger = logging.getLogger(__name__)


class DbtConfig(BaseSettings):
    """
    dbt configuration settings with automatic discovery and validation.

    Settings are loaded from:
    1. Environment variables (highest priority)
    2. .env file
    3. Automatic discovery (fallback)

    For containerised runtime deployment, set these environment variables:
    - DBT_PROFILES_DIR: Path to directory containing profiles.yml
    - DBT_PROJECT_DIR: Path to directory containing dbt_project.yml
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # dbt paths - will be auto-discovered if not set
    dbt_profiles_dir: Optional[str] = Field(
        default=None, description="Directory containing profiles.yml"
    )
    dbt_project_dir: Optional[str] = Field(
        default=None, description="Directory containing dbt_project.yml"
    )

    # Application settings
    dbt_target_default: str = Field(
        default="dev", description="Default dbt target if not specified in request"
    )

    @field_validator("dbt_profiles_dir", "dbt_project_dir")
    @classmethod
    def validate_paths_exist(cls, v: Optional[str]) -> Optional[str]:
        """Validate that configured paths exist."""
        if v is not None:
            path = Path(v)
            if not path.exists():
                raise ValueError(f"Path does not exist: {v}")
            if not path.is_dir():
                raise ValueError(f"Path is not a directory: {v}")
        return v

    def discover_paths(self) -> None:
        """
        Discover dbt configuration paths if not explicitly set.

        This method is called once at application startup to find
        dbt_project.yml and profiles.yml if they weren't provided
        via environment variables.

        Raises:
            DbtConfigurationError: If configuration files cannot be found
        """
        if not self.dbt_profiles_dir:
            logger.info("DBT_PROFILES_DIR not set, discovering profiles.yml...")
            self.dbt_profiles_dir = self._discover_profiles_dir()
            logger.info(f"Discovered profiles.yml at: {self.dbt_profiles_dir}")

        if not self.dbt_project_dir:
            logger.info("DBT_PROJECT_DIR not set, discovering dbt_project.yml...")
            self.dbt_project_dir = self._discover_project_dir()
            logger.info(f"Discovered dbt_project.yml at: {self.dbt_project_dir}")

    def _discover_profiles_dir(self) -> str:
        """
        Discover the directory containing profiles.yml.

        Returns:
            Path to directory containing profiles.yml

        Raises:
            DbtConfigurationError: If profiles.yml not found or duplicates exist
        """
        return _discover_config_file(filename="profiles.yml", file_type="profiles.yml")

    def _discover_project_dir(self) -> str:
        """
        Discover the directory containing dbt_project.yml.

        Returns:
            Path to directory containing dbt_project.yml

        Raises:
            DbtConfigurationError: If dbt_project.yml not found or duplicates exist
        """
        return _discover_config_file(
            filename="dbt_project.yml", file_type="dbt_project.yml"
        )

    def validate_configuration(self) -> None:
        """
        Validate that all required configuration files exist.

        This method should be called after discovery to ensure
        the configuration is valid before accepting requests.

        Raises:
            DbtConfigurationError: If configuration is invalid
        """
        profiles_path = Path(self.dbt_profiles_dir) / "profiles.yml"
        if not profiles_path.exists():
            raise create_configuration_missing_error(
                "profiles.yml", [str(self.dbt_profiles_dir)]
            )

        project_path = Path(self.dbt_project_dir) / "dbt_project.yml"
        if not project_path.exists():
            raise create_configuration_missing_error(
                "dbt_project.yml", [str(self.dbt_project_dir)]
            )

        logger.info("dbt configuration validated successfully")


def _discover_config_file(filename: str, file_type: str) -> str:
    """
    Discover a dbt configuration file.

    Search strategy (in order):
    1. Current working directory
    2. Common project locations
    3. Parent directories (up to 3 levels)
    4. Limited filesystem walk (excluded common dirs)

    Args:
        filename: Name of file to find (e.g., "profiles.yml")
        file_type: Type of file for error messages

    Returns:
        Path to directory containing the file

    Raises:
        DbtConfigurationError: If file not found or duplicates exist
    """
    from dbt_fastapi.params import PROJECT_ROOT

    EXCLUDED_DIRS = {
        ".venv",
        ".git",
        "__pycache__",
        ".pytest_cache",
        "logs",
        "dbt_internal_packages",
        "node_modules",
        "target",
        "dbt_packages",
        ".tox",
        "venv",
        "env",
    }

    # Strategy 1: Check current working directory first
    cwd = Path.cwd()
    if (cwd / filename).exists():
        logger.debug(f"Found {filename} in current directory: {cwd}")
        return str(cwd)

    # Strategy 2: Check common locations
    common_locations = [
        Path.cwd() / "placeholder_dbt_project",
        Path(__file__).parent.parent.parent / "placeholder_dbt_project",
        PROJECT_ROOT / "placeholder_dbt_project",
    ]

    for location in common_locations:
        if location.exists() and (location / filename).exists():
            logger.debug(f"Found {filename} in common location: {location}")
            return str(location.resolve())

    # Strategy 3: Check parent directories (limited depth)
    current = cwd
    for _ in range(3):  # Check up to 3 parent directories
        if (current / filename).exists():
            logger.debug(f"Found {filename} in parent directory: {current}")
            return str(current)
        current = current.parent
        if current == current.parent:  # Reached filesystem root
            break

    # Strategy 4: Limited filesystem walk (last resort)
    logger.debug(f"Performing limited filesystem walk to find {filename}...")
    found_paths: list[Path] = []
    search_paths: list[str] = []

    # Only walk from PROJECT_ROOT to limit scope
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # Prune excluded directories
        dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]

        root_path = Path(root)
        search_paths.append(str(root_path))

        if filename in files:
            found_paths.append(root_path.resolve())

    # Validate results
    if len(found_paths) == 0:
        logger.error(f"{file_type} not found in any searched paths")
        raise create_configuration_missing_error(file_type, search_paths)

    if len(found_paths) > 1:
        logger.error(f"Multiple {file_type} files found")
        raise create_configuration_duplicate_error(
            file_type, [str(path) for path in found_paths]
        )

    logger.info(f"Found {filename} at: {found_paths[0]}")
    return str(found_paths[0])


# Global configuration instance - Singleton
_config: Optional[DbtConfig] = None


@lru_cache(maxsize=1)
def get_dbt_config() -> DbtConfig:
    """
    Get the cached dbt configuration.

    This function returns a singleton instance of DbtConfig that is
    initialized once at application startup and cached for all requests.

    Returns:
        DbtConfig instance with discovered/configured paths

    Raises:
        RuntimeError: If called before configuration is initialized
    """
    global _config

    if _config is None:
        raise RuntimeError(
            "Configuration not initialized. "
            "Call initialize_dbt_config() during application startup."
        )

    return _config


def initialize_dbt_config() -> DbtConfig:
    """
    Initialize and cache the dbt configuration.

    This function should be called once during application startup
    (in a FastAPI startup event handler). It discovers configuration
    paths, validates them, and caches the result for all requests.

    Returns:
        Initialized and validated DbtConfig instance

    Raises:
        DbtConfigurationError: If configuration is invalid
    """
    global _config

    logger.info("Initializing dbt configuration...")

    # Create config from environment/settings
    config = DbtConfig()

    # Discover paths if not set via environment variables
    config.discover_paths()

    # Validate configuration
    config.validate_configuration()

    # Cache globally
    _config = config

    # Also cache in lru_cache by calling get_dbt_config
    get_dbt_config.cache_clear()  # Clear any previous cache

    logger.info(
        f"dbt configuration initialized successfully:\n"
        f"  profiles_dir: {config.dbt_profiles_dir}\n"
        f"  project_dir: {config.dbt_project_dir}\n"
        f"  default_target: {config.dbt_target_default}"
    )

    return config


def reset_dbt_config() -> None:
    """
    Reset the cached configuration.

    This function is primarily for testing purposes, allowing tests
    to reset the configuration state between test runs.
    """
    global _config
    _config = None
    get_dbt_config.cache_clear()
    logger.debug("dbt configuration cache cleared")
