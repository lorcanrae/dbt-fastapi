from typing import Any
from fastapi import status

# Error hierarchy:
#
# DbtFastApiError (base)
# ├── DbtValidationError    (400)
# │   └── DbtTargetError    (400)
# ├── DbtConfigurationError (400)
# ├── DbtCompilationError   (400)
# ├── DbtExecutionError     (500)
# └── DbtInternalError      (500)


class DbtFastApiError(Exception):
    """
    Base exception for all dbt-fastapi wrapper errors.

    Follows the principle of creating domain-specific exceptions that are
    independent of the underlying dbt implementation.
    """

    def __init__(
        self,
        message: str,
        http_status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        self.message = message
        self.http_status_code = http_status_code
        self.details = details or {}
        self.original_exception = original_exception
        super().__init__(self.message)


# === Client Error Exceptions (4xx) ===


class DbtValidationError(DbtFastApiError):
    """Raised when user input validation fails."""

    def __init__(
        self,
        message: str,
        field: str | None = None,
        details: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        details = details or {}
        if field:
            details["field"] = field

        super().__init__(
            message=message,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
            original_exception=original_exception,
        )


class DbtTargetError(DbtValidationError):
    """Raised when an invalid dbt target is specified."""

    def __init__(
        self,
        provided_target: str,
        valid_targets: list[str] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        valid_targets = valid_targets or []
        message = f"Invalid dbt target '{provided_target}'"
        if valid_targets:
            message += f". Valid targets: {', '.join(valid_targets)}"

        super().__init__(
            message=message,
            field="target",
            details={
                "provided_target": provided_target,
                "valid_targets": valid_targets,
                "suggestion": f"Use one of: {', '.join(valid_targets)}"
                if valid_targets
                else "Check your profiles.yml configuration",
            },
            original_exception=original_exception,
        )


class DbtConfigurationError(DbtFastApiError):
    """Raised when dbt configuration is invalid or missing."""

    def __init__(
        self,
        message: str,
        config_type: str | None = None,
        config_path: str | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        details = {}
        if config_type:
            details["config_type"] = config_type
        if config_path:
            details["config_path"] = config_path

        super().__init__(
            message=message,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
            original_exception=original_exception,
        )


class DbtCompilationError(DbtFastApiError):
    """Raised when dbt models fail to compile due to SQL syntax errors."""

    def __init__(
        self,
        message: str,
        failed_models: list[dict[str, Any]] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        failed_models = failed_models or []

        details = {
            "failed_models": failed_models,
            "suggestion": "Check your SQL syntax in the failing models",
        }

        super().__init__(
            message=message,
            http_status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
            original_exception=original_exception,
        )


# === Server Error Exceptions (5xx) ===


class DbtExecutionError(DbtFastApiError):
    """Raised when dbt command execution fails."""

    def __init__(
        self,
        message: str,
        command: list[str] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        details = {}
        if command:
            details["command"] = " ".join(command)
            details["suggestion"] = "Check dbt logs for detailed error information"

        super().__init__(
            message=message,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details,
            original_exception=original_exception,
        )


class DbtInternalError(DbtFastApiError):
    """Raised for unexpected dbt-related errors."""

    def __init__(
        self,
        message: str = "An unexpected error occurred during dbt execution",
        original_exception: Exception | None = None,
    ) -> None:
        super().__init__(
            message=message,
            http_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={
                "suggestion": "This may be a temporary issue. Please try again or contact support if the problem persists."
            },
            original_exception=original_exception,
        )


# === Exception Translation Functions ===


def translate_dbt_exception(
    dbt_exception: Exception, context: dict[str, Any] | None = None
) -> DbtFastApiError:
    """
    Translate dbt exceptions into domain-specific exceptions.

    This function encapsulates all the knowledge about dbt's exception types
    and maps them to this application's exception hierarchy.

    Args:
        dbt_exception: The original dbt exception
        context: Additional context (e.g., command, target, selection criteria)

    Returns:
        Relevant DbtFastApiError subclass
    """
    from dbt.exceptions import (
        DbtRuntimeError,
        DbtProjectError,
        DbtProfileError,
        ParsingError,
    )

    context = context or {}
    error_message = str(dbt_exception)

    # Handle specific dbt exception types
    if isinstance(dbt_exception, ParsingError):
        return DbtConfigurationError(
            message=f"Configuration parsing error: {error_message}",
            config_type="parsing",
            original_exception=dbt_exception,
        )

    elif isinstance(dbt_exception, DbtProjectError):
        return DbtConfigurationError(
            message=f"dbt project configuration error: {error_message}",
            config_type="dbt_project.yml",
            config_path=context.get("project_dir"),
            original_exception=dbt_exception,
        )

    elif isinstance(dbt_exception, DbtProfileError):
        # Check if it's a target error
        if "does not have a target named" in error_message:
            import re

            valid_targets = re.findall(r"- (\w+)", error_message)
            return DbtTargetError(
                provided_target=context.get("target", "unknown"),
                valid_targets=valid_targets,
                original_exception=dbt_exception,
            )

        return DbtConfigurationError(
            message=f"dbt profile configuration error: {error_message}",
            config_type="profiles.yml",
            config_path=context.get("profiles_dir"),
            original_exception=dbt_exception,
        )

    elif isinstance(dbt_exception, DbtRuntimeError):
        # Check for specific runtime error patterns
        if "does not have a target named" in error_message:
            import re

            valid_targets = re.findall(r"- (\w+)", error_message)
            return DbtTargetError(
                provided_target=context.get("target", "unknown"),
                valid_targets=valid_targets,
                original_exception=dbt_exception,
            )

        # Generic runtime error
        return DbtExecutionError(
            message=f"dbt execution failed: {error_message}",
            command=context.get("command"),
            original_exception=dbt_exception,
        )

    # Fallback for unknown dbt exceptions
    return DbtInternalError(
        message=f"Unexpected dbt error: {error_message}",
        original_exception=dbt_exception,
    )


# === Factory Functions ===


def create_compilation_error(
    failed_models: list[dict[str, Any]],
) -> DbtCompilationError:
    """
    Factory function for craeting compilation errors.

    Args:
        failed_mdoels: List of models that failed compilation with error details
    """
    model_count = len(failed_models)
    model_names = [model["name"] for model in failed_models]

    if model_count == 1:
        message = f"SQL compilation failed for model '{model_names[0]}'"
    else:
        message = (
            f"SQL compilation failed for {model_count} models: {', '.join(model_names)}"
        )

    return DbtCompilationError(message=message, failed_models=failed_models)


def create_configuration_missing_error(
    config_file: str, search_paths: list[str] | None = None
) -> DbtConfigurationError:
    """
    Factory function for creating configuration file missing errors.
    """
    message = f"Required configuration file '{config_file}' not found"
    if search_paths:
        message += f" in paths: {', '.join(search_paths)}"

    error = DbtConfigurationError(
        message=message,
        config_type=config_file,
    )

    if search_paths:
        error.details["search_paths"] = search_paths

    return error


def create_configuration_duplicate_error(
    config_file: str, found_paths: list[str]
) -> DbtConfigurationError:
    """
    Factory function for creating configuration file duplication errors.
    """

    error = DbtConfigurationError(
        message=f"Multiple '{config_file}' files found. Please ensure that only one exists in your project.",
        config_type=config_file,
    )

    error.details.update(
        {
            "found_paths": found_paths,
            "suggestion": "Remove duplicate configuration files or use DBT_PROJECT_DIR/DBT_PROFILES_DIR environment variables",
        }
    )

    return error
