"""Tests for clean exception design in dbt-fastapi wrapper."""

import pytest
from fastapi import HTTPException
from unittest.mock import Mock, patch

from dbt.exceptions import (
    DbtRuntimeError,
    DbtProjectError,
    DbtProfileError,
    ParsingError,
)

from dbt_fastapi.exceptions import (
    DbtFastApiError,
    DbtValidationError,
    DbtModelSelectionError,
    DbtTargetError,
    DbtConfigurationError,
    DbtExecutionError,
    DbtInternalError,
    translate_dbt_exception,
    create_model_selection_error,
    create_execution_failure_error,
    create_configuration_missing_error,
    create_configuration_duplicate_error,
)
from dbt_fastapi.dbt_manager import DbtManager


class TestCustomExceptionHierarchy:
    """Test the custom exception hierarchy design."""

    def test_base_exception_structure(self) -> None:
        """Test that base exception has proper structure."""
        error = DbtFastApiError(
            message="Test error",
            http_status_code=400,
            details={"key": "value"},
        )

        assert error.message == "Test error"
        assert error.http_status_code == 400
        assert error.details == {"key": "value"}
        assert str(error) == "Test error"

    def test_validation_error_inheritance(self) -> None:
        """Test that validation errors inherit properly."""
        error = DbtValidationError("Invalid input", field="target")

        assert isinstance(error, DbtFastApiError)
        assert error.http_status_code == 400
        assert error.details["field"] == "target"

    def test_model_selection_error_factory(self) -> None:
        """Test model selection error factory."""
        error = create_model_selection_error("--select model1")

        assert isinstance(error, DbtModelSelectionError)
        assert isinstance(error, DbtValidationError)
        assert "No nodes matched" in error.message
        assert error.details["selection_criteria"] == "--select model1"
        assert "Check your" in error.details["suggestion"]

    def test_target_error_with_suggestions(self) -> None:
        """Test target error includes helpful suggestions."""
        error = DbtTargetError(provided_target="invalid", valid_targets=["dev", "prod"])

        assert "Invalid dbt target 'invalid'" in error.message
        assert error.details["provided_target"] == "invalid"
        assert error.details["valid_targets"] == ["dev", "prod"]
        assert "Use one of: dev, prod" in error.details["suggestion"]

    def test_configuration_error_factories(self) -> None:
        """Test configuration error factory functions."""
        # Missing file error
        missing_error = create_configuration_missing_error(
            "dbt_project.yml", ["/path1", "/path2"]
        )
        assert "not found" in missing_error.message
        assert missing_error.details["search_paths"] == ["/path1", "/path2"]

        # Duplicate file error
        duplicate_error = create_configuration_duplicate_error(
            "profiles.yml", ["/path1/profiles.yml", "/path2/profiles.yml"]
        )
        assert "Multiple" in duplicate_error.message
        assert "Remove duplicate" in duplicate_error.details["suggestion"]

    def test_execution_error_factory(self) -> None:
        """Test execution error factory."""
        error = create_execution_failure_error(["run", "--target", "dev"])

        assert isinstance(error, DbtExecutionError)
        assert error.http_status_code == 500
        assert error.details["command"] == "run --target dev"
        assert "Check dbt logs" in error.details["suggestion"]


class TestDbtExceptionTranslation:
    """Test translation of dbt exceptions to our custom exceptions."""

    def test_translate_parsing_error(self) -> None:
        """Test that ParsingError is properly translated."""
        dbt_error = ParsingError("Invalid YAML syntax")
        context = {"project_dir": "/path/to/project"}

        translated = translate_dbt_exception(dbt_error, context)

        assert isinstance(translated, DbtConfigurationError)
        assert "Configuration parsing error" in translated.message
        assert translated.details["config_type"] == "parsing"
        assert translated.original_exception is dbt_error

    def test_translate_profile_error_target(self) -> None:
        """Test that DbtProfileError with target info is translated to DbtTargetError."""
        dbt_error = DbtProfileError(
            "Profile 'default' does not have a target named 'invalid'. - dev\n- prod"
        )
        context = {"target": "invalid"}

        translated = translate_dbt_exception(dbt_error, context)

        assert isinstance(translated, DbtTargetError)
        assert translated.details["provided_target"] == "invalid"
        assert "dev" in translated.details["valid_targets"]
        assert "prod" in translated.details["valid_targets"]

    def test_translate_profile_error_generic(self) -> None:
        """Test that generic DbtProfileError is translated to DbtConfigurationError."""
        dbt_error = DbtProfileError("Connection failed")
        context = {"profiles_dir": "/path/to/profiles"}

        translated = translate_dbt_exception(dbt_error, context)

        assert isinstance(translated, DbtConfigurationError)
        assert "profile configuration error" in translated.message
        assert translated.details["config_type"] == "profiles.yml"
        assert translated.details["config_path"] == "/path/to/profiles"

    def test_translate_project_error(self) -> None:
        """Test that DbtProjectError is properly translated."""
        dbt_error = DbtProjectError("Invalid project configuration")
        context = {"project_dir": "/path/to/project"}

        translated = translate_dbt_exception(dbt_error, context)

        assert isinstance(translated, DbtConfigurationError)
        assert "project configuration error" in translated.message
        assert translated.details["config_type"] == "dbt_project.yml"

    def test_translate_runtime_error_target(self) -> None:
        """Test that DbtRuntimeError with target info becomes DbtTargetError."""
        dbt_error = DbtRuntimeError(
            "Profile 'default' does not have a target named 'test'. - dev\n- prod"
        )
        context = {"target": "test"}

        translated = translate_dbt_exception(dbt_error, context)

        assert isinstance(translated, DbtTargetError)
        assert translated.details["provided_target"] == "test"

    def test_translate_runtime_error_model_selection(self) -> None:
        """Test that DbtRuntimeError with model selection context becomes DbtModelSelectionError."""
        dbt_error = DbtRuntimeError("No nodes found")
        context = {
            "no_models_matched": True,
            "selection_criteria": "--select nonexistent_model",
        }

        translated = translate_dbt_exception(dbt_error, context)

        assert isinstance(translated, DbtModelSelectionError)
        assert translated.details["selection_criteria"] == "--select nonexistent_model"

    def test_translate_runtime_error_generic(self) -> None:
        """Test that generic DbtRuntimeError becomes DbtExecutionError."""
        dbt_error = DbtRuntimeError("Model compilation failed")
        context = {"command": ["run", "--target", "dev"]}

        translated = translate_dbt_exception(dbt_error, context)

        assert isinstance(translated, DbtExecutionError)
        assert "dbt execution failed" in translated.message
        # Command is stored as a joined string in DbtExecutionError
        assert translated.details["command"] == "run --target dev"

    def test_translate_unknown_exception(self) -> None:
        """Test that unknown exceptions become DbtInternalError."""
        unknown_error = ValueError("Some unexpected error")

        translated = translate_dbt_exception(unknown_error)

        assert isinstance(translated, DbtInternalError)
        assert "Unexpected dbt error" in translated.message
        assert translated.original_exception is unknown_error


class TestDbtManagerWithCleanExceptions:
    """Test DbtManager using the clean exception design."""

    @patch("os.walk")
    def test_model_selection_error_handling(self, mock_walk: Mock) -> None:
        """Test that model selection errors are properly handled."""
        # Mock config file discovery
        mock_walk.return_value = [("/project", [], ["dbt_project.yml", "profiles.yml"])]

        manager = DbtManager(verb="run", target="dev", select_args="nonexistent_model")

        # Mock a successful result with no models
        mock_result = Mock()
        mock_result.success = True
        mock_result.result = Mock()
        mock_result.result.results = []  # No models found

        with patch.object(manager.runner, "invoke", return_value=mock_result):
            with pytest.raises(HTTPException) as exc_info:
                manager.execute_dbt_command()

            # Verify the HTTP exception structure
            assert exc_info.value.status_code == 400
            assert exc_info.value.detail["error"] == "DbtModelSelectionError"
            assert "No nodes matched" in exc_info.value.detail["message"]
            assert (
                exc_info.value.detail["selection_criteria"]
                == "select_args: nonexistent_model"
            )
            assert "Check your" in exc_info.value.detail["suggestion"]

    @patch("os.walk")
    def test_execution_failure_handling(self, mock_walk: Mock) -> None:
        """Test that execution failures are properly handled."""
        # Mock config file discovery
        mock_walk.return_value = [("/project", [], ["dbt_project.yml", "profiles.yml"])]

        manager = DbtManager(verb="run", target="dev")

        # Mock a failed result
        mock_result = Mock()
        mock_result.success = False

        with patch.object(manager.runner, "invoke", return_value=mock_result):
            with pytest.raises(HTTPException) as exc_info:
                manager.execute_dbt_command()

            assert exc_info.value.status_code == 500
            assert exc_info.value.detail["error"] == "DbtExecutionError"
            assert "dbt command execution failed" in exc_info.value.detail["message"]
            assert "Check dbt logs" in exc_info.value.detail["suggestion"]

    @patch("os.walk")
    def test_dbt_exception_translation(self, mock_walk: Mock) -> None:
        """Test that dbt exceptions are properly translated."""
        # Mock config file discovery
        mock_walk.return_value = [("/project", [], ["dbt_project.yml", "profiles.yml"])]

        manager = DbtManager(verb="run", target="invalid_target")

        # Mock a DbtProfileError being raised
        profile_error = DbtProfileError(
            "Profile 'default' does not have a target named 'invalid_target'. - dev\n- prod"
        )

        with patch.object(manager.runner, "invoke", side_effect=profile_error):
            with pytest.raises(HTTPException) as exc_info:
                manager.execute_dbt_command()

            assert exc_info.value.status_code == 400
            assert exc_info.value.detail["error"] == "DbtTargetError"
            assert exc_info.value.detail["provided_target"] == "invalid_target"
            assert "dev" in exc_info.value.detail["valid_targets"]
            assert "Use one of: dev, prod" in exc_info.value.detail["suggestion"]

    @patch("os.walk")
    def test_configuration_missing_error(self, mock_walk: Mock) -> None:
        """Test that missing configuration files raise appropriate errors."""
        # Mock no dbt_project.yml found
        mock_walk.return_value = [("/root", [], ["other_file.txt"])]

        with pytest.raises(HTTPException) as exc_info:
            DbtManager(verb="run", target="dev")

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail["error"] == "DbtConfigurationError"
        assert "dbt_project.yml" in exc_info.value.detail["message"]
        assert "not found" in exc_info.value.detail["message"]

    @patch("os.walk")
    def test_configuration_duplicate_error(self, mock_walk: Mock) -> None:
        """Test that duplicate configuration files raise appropriate errors."""
        # Mock multiple dbt_project.yml files found
        mock_walk.return_value = [
            ("/root1", [], ["dbt_project.yml", "profiles.yml"]),
            ("/root2", [], ["dbt_project.yml"]),
        ]

        with pytest.raises(HTTPException) as exc_info:
            DbtManager(verb="run", target="dev")

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail["error"] == "DbtConfigurationError"
        assert "Multiple" in exc_info.value.detail["message"]
        assert "Remove duplicate" in exc_info.value.detail["suggestion"]

    def test_unsafe_command_exception_handling(self) -> None:
        """Test that unsafe commands handle exceptions properly."""
        with patch("dbt_fastapi.dbt_manager.dbtRunner") as mock_runner_class:
            mock_runner = Mock()
            mock_runner_class.return_value = mock_runner

            # Mock a parsing error
            parsing_error = ParsingError("Invalid YAML syntax")
            mock_runner.invoke.side_effect = parsing_error

            with pytest.raises(HTTPException) as exc_info:
                DbtManager.execute_unsafe_dbt_command(["dbt", "run"])

            assert exc_info.value.status_code == 400
            assert exc_info.value.detail["error"] == "DbtConfigurationError"
            assert "Configuration parsing error" in exc_info.value.detail["message"]


class TestExceptionDesignBenefits:
    """Test cases that demonstrate the benefits of the clean exception design."""

    def test_exception_chaining_preserves_original(self) -> None:
        """Test that original dbt exceptions are preserved for debugging."""
        original_error = DbtRuntimeError("Original dbt error")
        context = {"command": ["run"]}

        translated = translate_dbt_exception(original_error, context)

        assert translated.original_exception is original_error
        assert isinstance(translated.original_exception, DbtRuntimeError)

    def test_consistent_error_structure(self) -> None:
        """Test that all custom exceptions have consistent structure."""
        errors = [
            create_model_selection_error("--select model1"),
            DbtTargetError("invalid", ["dev", "prod"]),
            create_configuration_missing_error("dbt_project.yml"),
            create_execution_failure_error(["run"]),
        ]

        for error in errors:
            # All should be DbtFastApiError subclasses
            assert isinstance(error, DbtFastApiError)

            # All should have required attributes
            assert hasattr(error, "message")
            assert hasattr(error, "http_status_code")
            assert hasattr(error, "details")

            # All should have appropriate HTTP status codes
            assert error.http_status_code in [400, 500]

            # All should have helpful details
            assert isinstance(error.details, dict)

    def test_error_categorization(self) -> None:
        """Test that errors are properly categorized by HTTP status codes."""
        # Client errors (400)
        client_errors = [
            create_model_selection_error("--select model1"),
            DbtTargetError("invalid", ["dev"]),
            create_configuration_missing_error("dbt_project.yml"),
        ]

        for error in client_errors:
            assert error.http_status_code == 400

        # Server errors (500)
        server_errors = [
            create_execution_failure_error(["run"]),
            DbtInternalError("Unexpected error"),
        ]

        for error in server_errors:
            assert error.http_status_code == 500

    def test_helpful_error_messages(self) -> None:
        """Test that error messages include helpful suggestions."""
        # Model selection error should suggest checking arguments
        model_error = create_model_selection_error("--select nonexistent")
        assert "Check your" in model_error.details["suggestion"]

        # Target error should suggest valid targets
        target_error = DbtTargetError("invalid", ["dev", "prod"])
        assert "Use one of: dev, prod" in target_error.details["suggestion"]

        # Configuration error should suggest solutions
        duplicate_error = create_configuration_duplicate_error(
            "profiles.yml", ["/path1", "/path2"]
        )
        assert "Remove duplicate" in duplicate_error.details["suggestion"]

    def test_testability_benefits(self) -> None:
        """Test that custom exceptions are easier to test than HTTP exceptions."""
        # We can test business logic without HTTP concerns
        with pytest.raises(DbtModelSelectionError) as exc_info:
            raise create_model_selection_error("--select nonexistent")

        # We can inspect the error details directly
        error = exc_info.value
        assert error.details["selection_criteria"] == "--select nonexistent"
        assert "Check your" in error.details["suggestion"]

        # We can test error categorization
        assert error.http_status_code == 400
        assert isinstance(error, DbtValidationError)

    def test_maintainability_benefits(self) -> None:
        """Test that the design supports easy maintenance and extension."""

        # Easy to add new error types
        class DbtCustomError(DbtFastApiError):
            def __init__(self, custom_field: str) -> None:
                super().__init__(
                    message=f"Custom error: {custom_field}",
                    http_status_code=400,
                    details={"custom_field": custom_field},
                )

        custom_error = DbtCustomError("test_value")
        assert custom_error.message == "Custom error: test_value"
        assert custom_error.details["custom_field"] == "test_value"
        assert isinstance(custom_error, DbtFastApiError)

    def test_debugging_benefits(self) -> None:
        """Test that the design provides better debugging information."""
        original_dbt_error = DbtRuntimeError("Original error message")
        context = {
            "target": "dev",
            "command": ["run", "--select", "model1"],
            "selection_criteria": "--select model1",
        }

        translated = translate_dbt_exception(original_dbt_error, context)

        # Rich debugging information
        assert translated.original_exception is original_dbt_error
        assert "command" in translated.details
        # Command is stored as a joined string in DbtExecutionError
        assert translated.details["command"] == "run --select model1"

        # Clear error hierarchy
        assert isinstance(translated, DbtExecutionError)
        assert isinstance(translated, DbtFastApiError)
