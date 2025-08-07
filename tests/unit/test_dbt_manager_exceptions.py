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
    DbtTargetError,
    DbtConfigurationError,
    DbtExecutionError,
    DbtInternalError,
    translate_dbt_exception,
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

    def test_execution_error_factory(self) -> None:
        """Test execution error factory."""
        error = create_execution_failure_error(["run", "--target", "dev"])

        assert isinstance(error, DbtExecutionError)
        assert error.http_status_code == 500
        assert error.details["command"] == "run --target dev"


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
    def test_successful_execution_with_empty_results(self, mock_walk: Mock) -> None:
        """Test that empty results no longer raise DbtModelSelectionError."""
        # Mock config file discovery
        mock_walk.return_value = [("/project", [], ["dbt_project.yml", "profiles.yml"])]

        manager = DbtManager(verb="run", target="dev", select_args="nonexistent_model")

        # Mock a successful result with no models
        mock_result = Mock()
        mock_result.success = True
        mock_result.result = Mock()
        mock_result.result.results = []  # No models found

        with patch.object(manager.runner, "invoke", return_value=mock_result):
            # This should NOT raise an exception anymore
            result = manager.execute_dbt_command()
            nodes = manager.get_nodes_from_result(result)

            assert result.success is True
            assert len(nodes) == 0  # Empty list is returned, not an exception

    @patch("os.walk")
    def test_execution_failure_handling(self, mock_walk: Mock) -> None:
        """Test that execution failures are properly handled."""
        # Mock config file discovery
        mock_walk.return_value = [("/project", [], ["dbt_project.yml", "profiles.yml"])]

        manager = DbtManager(verb="run", target="dev")

        # Mock a failed result
        mock_result = Mock(success=False, exception=None, spec=["success", "exception"])

        with patch.object(manager.runner, "invoke", return_value=mock_result):
            with pytest.raises(HTTPException) as exc_info:
                manager.execute_dbt_command()

            assert exc_info.value.status_code == 500
            assert exc_info.value.detail["error"] == "DbtExecutionError"
            assert "dbt command execution failed" in exc_info.value.detail["message"]

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


class TestNodeExtraction:
    """Test the new unified node extraction functionality."""

    def test_get_nodes_from_list_result(self) -> None:
        """Test node extraction from list command results."""
        manager = Mock()

        # Mock list result
        mock_result = Mock()
        mock_result.result = ["model.project.model1", "model.project.model2"]

        from dbt_fastapi.dbt_manager import DbtManager

        nodes = DbtManager.get_nodes_from_result(manager, mock_result)

        assert nodes == ["model.project.model1", "model.project.model2"]

    def test_get_nodes_from_execution_result(self) -> None:
        """Test node extraction from execution command results."""
        manager = Mock()

        # Mock execution result with run results
        mock_result = Mock()
        mock_result.result = Mock()

        mock_run_result1 = Mock()
        mock_run_result1.node = Mock()
        mock_run_result1.node.unique_id = "model.project.model1"

        mock_run_result2 = Mock()
        mock_run_result2.node = Mock()
        mock_run_result2.node.unique_id = "test.project.test1"

        mock_result.result.results = [mock_run_result1, mock_run_result2]

        from dbt_fastapi.dbt_manager import DbtManager

        nodes = DbtManager.get_nodes_from_result(manager, mock_result)

        assert nodes == ["model.project.model1", "test.project.test1"]

    def test_get_nodes_from_empty_result(self) -> None:
        """Test node extraction from empty results."""
        manager = Mock()

        # Mock empty result
        mock_result = Mock()
        mock_result.result = Mock()
        mock_result.result.results = []

        from dbt_fastapi.dbt_manager import DbtManager

        nodes = DbtManager.get_nodes_from_result(manager, mock_result)

        assert nodes == []


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

    def test_testability_benefits(self) -> None:
        """Test that custom exceptions are easier to test than HTTP exceptions."""
        # We can test business logic without HTTP concerns
        with pytest.raises(DbtTargetError) as exc_info:
            raise DbtTargetError("invalid", ["dev", "prod"])

        # We can inspect the error details directly
        error = exc_info.value
        assert error.details["provided_target"] == "invalid"

        # We can test error categorization
        assert error.http_status_code == 400
        assert isinstance(error, DbtValidationError)

    def test_debugging_benefits(self) -> None:
        """Test that the design provides better debugging information."""
        original_dbt_error = DbtRuntimeError("Original error message")
        context = {
            "target": "dev",
            "command": ["run", "--select", "model1"],
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
