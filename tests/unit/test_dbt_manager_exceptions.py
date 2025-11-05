"""
Tests for DbtManager with configuration dependency injection.

Key changes from original:
- DbtManager now requires profiles_dir and project_dir parameters
- No filesystem walking in DbtManager
- Tests use explicit paths instead of relying on discovery
"""

import pytest
from unittest.mock import Mock, patch

from dbt.exceptions import (
    DbtProfileError,
    ParsingError,
)

from dbt_fastapi.exceptions import (
    DbtTargetError,
    DbtConfigurationError,
    DbtExecutionError,
)
from dbt_fastapi.dbt_manager import DbtManager


class TestDbtManagerInitialization:
    """Test DbtManager initialization with explicit configuration."""

    def test_manager_accepts_explicit_paths(self, dummy_paths):
        """Test that DbtManager accepts explicit configuration paths."""
        profiles_dir, project_dir = dummy_paths

        # Should not raise
        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        assert manager.profiles_yaml_dir == profiles_dir
        assert manager.dbt_project_yaml_dir == project_dir
        assert manager.verb == "run"
        assert manager.target == "dev"

    def test_manager_with_selection_args(self, dummy_paths):
        """Test DbtManager with selection arguments."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
            select_args="model1 model2",
            exclude_args="model3",
            selector_args="my_selector",
        )

        assert manager.select_args == ["model1", "model2"]
        assert manager.exclude_args == ["model3"]
        assert manager.selector_args == ["my_selector"]

    def test_manager_generates_correct_cli_args(self, dummy_paths):
        """Test that DbtManager generates correct CLI arguments."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
            select_args="model1",
        )

        # Check CLI args
        assert "run" in manager.dbt_cli_args
        assert "--target" in manager.dbt_cli_args
        assert "dev" in manager.dbt_cli_args
        assert "--select" in manager.dbt_cli_args
        assert "model1" in manager.dbt_cli_args
        assert "--project-dir" in manager.dbt_cli_args
        assert project_dir in manager.dbt_cli_args
        assert "--profiles-dir" in manager.dbt_cli_args
        assert profiles_dir in manager.dbt_cli_args


class TestDbtManagerNoFilesystemWalking:
    """Test that DbtManager doesn't walk filesystem."""

    def test_no_filesystem_walking_on_initialization(self, dummy_paths):
        """Test that DbtManager doesn't call os.walk during initialization."""
        profiles_dir, project_dir = dummy_paths

        # Mock os.walk to verify it's never called
        with patch("os.walk") as mock_walk:
            _manager = DbtManager(
                verb="run",
                target="dev",
                profiles_dir=profiles_dir,
                project_dir=project_dir,
            )

            # os.walk should NEVER be called
            mock_walk.assert_not_called()

    def test_multiple_instances_no_repeated_discovery(self, dummy_paths):
        """Test that creating multiple managers doesn't trigger discovery."""
        profiles_dir, project_dir = dummy_paths

        with patch("os.walk") as mock_walk:
            # Create 100 managers (simulating 100 requests)
            managers = []
            for _ in range(100):
                manager = DbtManager(
                    verb="run",
                    target="dev",
                    profiles_dir=profiles_dir,
                    project_dir=project_dir,
                )
                managers.append(manager)

            # os.walk should NEVER be called
            assert mock_walk.call_count == 0

            # All should have same paths
            assert all(m.profiles_yaml_dir == profiles_dir for m in managers)
            assert all(m.dbt_project_yaml_dir == project_dir for m in managers)


class TestDbtManagerExecution:
    """Test DbtManager command execution."""

    def test_successful_execution(self, dummy_paths, mock_dbt_result):
        """Test successful dbt command execution."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        # Mock the runner
        with patch.object(manager.runner, "invoke", return_value=mock_dbt_result):
            result = manager.execute_dbt_command()

            assert result.success is True

    def test_execution_failure_raises_error(self, dummy_paths, mock_failed_dbt_result):
        """Test that failed execution raises DbtExecutionError."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        with patch.object(
            manager.runner, "invoke", return_value=mock_failed_dbt_result
        ):
            with pytest.raises(DbtExecutionError) as exc:
                manager.execute_dbt_command()

            assert exc.value.http_status_code == 500

    def test_invalid_target_raises_target_error(self, dummy_paths):
        """Test that invalid target raises DbtTargetError."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="invalid_target",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        # Mock DbtProfileError with target info
        profile_error = DbtProfileError(
            "Profile 'default' does not have a target named 'invalid_target'. - dev\n- prod"
        )

        with patch.object(manager.runner, "invoke", side_effect=profile_error):
            with pytest.raises(DbtTargetError) as exc:
                manager.execute_dbt_command()

            assert exc.value.details["provided_target"] == "invalid_target"
            assert "dev" in exc.value.details["valid_targets"]
            assert "prod" in exc.value.details["valid_targets"]

    def test_parsing_error_translated(self, dummy_paths):
        """Test that ParsingError is translated to DbtConfigurationError."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        parsing_error = ParsingError("Invalid YAML syntax")

        with patch.object(manager.runner, "invoke", side_effect=parsing_error):
            with pytest.raises(DbtConfigurationError) as exc:
                manager.execute_dbt_command()

            assert "Configuration parsing error" in exc.value.message


class TestDbtManagerUnsafeCommand:
    """Test unsafe command execution."""

    def test_unsafe_command_execution(self, mock_dbt_result):
        """Test execute_unsafe_dbt_command."""
        with patch("dbt_fastapi.dbt_manager.dbtRunner") as mock_runner_class:
            mock_runner = Mock()
            mock_runner.invoke.return_value = mock_dbt_result
            mock_runner_class.return_value = mock_runner

            result = DbtManager.execute_unsafe_dbt_command(["dbt", "run"])

            assert result.success is True
            # Should have removed 'dbt' from command
            mock_runner.invoke.assert_called_once()
            called_args = mock_runner.invoke.call_args[0][0]
            assert "dbt" not in called_args or called_args[0] != "dbt"

    def test_unsafe_command_removes_dbt_prefix(self, mock_dbt_result):
        """Test that 'dbt' is removed from command."""
        with patch("dbt_fastapi.dbt_manager.dbtRunner") as mock_runner_class:
            mock_runner = Mock()
            mock_runner.invoke.return_value = mock_dbt_result
            mock_runner_class.return_value = mock_runner

            DbtManager.execute_unsafe_dbt_command(["dbt", "list", "--select", "model1"])

            called_args = mock_runner.invoke.call_args[0][0]
            # First element should be 'list', not 'dbt'
            assert called_args[0] == "list"


class TestNodeExtraction:
    """Test node extraction from dbt results."""

    def test_get_nodes_from_empty_result(self, dummy_paths):
        """Test node extraction from empty result."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        mock_result = Mock()
        mock_result.result = Mock()
        mock_result.result.results = []

        nodes = manager.get_nodes_from_result(mock_result)

        assert nodes == []

    def test_get_nodes_from_list_result(self, dummy_paths):
        """Test node extraction from list command."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="list",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        # Mock list result (returns JSON strings)
        mock_result = Mock()
        mock_result.result = [
            '{"unique_id": "model.project.model1", "resource_type": "model", "alias": "model1", "depends_on": {"nodes": []}}',
            '{"unique_id": "model.project.model2", "resource_type": "model", "alias": "model2", "depends_on": {"nodes": ["model.project.model1"]}}',
        ]

        nodes = manager.get_nodes_from_result(mock_result)

        assert len(nodes) == 2
        # Should be a DbtNode object, not a dictionary
        assert nodes[0].unique_id == "model.project.model1"
        assert nodes[1].unique_id == "model.project.model2"

    def test_get_nodes_from_run_result(self, dummy_paths):
        """Test node extraction from run command."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        # Mock run result
        mock_result = Mock()
        mock_result.result = Mock()

        mock_node1 = Mock()
        mock_node1.unique_id = "model.project.model1"
        mock_node1.resource_type = "model"
        mock_node1.fqn = ["project", "model1"]
        mock_node1.depends_on = Mock()
        mock_node1.depends_on.nodes = []

        mock_run_result1 = Mock()
        mock_run_result1.node = mock_node1

        mock_result.result.results = [mock_run_result1]

        nodes = manager.get_nodes_from_result(mock_result)

        assert len(nodes) == 1
        # This should be a DbtNode, not a dictionary
        assert nodes[0].unique_id == "model.project.model1"
        assert nodes[0].fqn == "project.model1"


class TestTestSummary:
    """Test test summary extraction."""

    def test_get_test_summary_for_test_command(self, dummy_paths):
        """Test that get_test_summary works for test command."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="test",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        # Mock test result
        mock_result = Mock()
        mock_result.result = Mock()
        mock_result.result.results = []

        summary = manager.get_test_summary(mock_result)

        assert summary is not None
        assert "total" in summary
        assert "passed" in summary
        assert "failed" in summary

    def test_get_test_summary_for_non_test_command(self, dummy_paths):
        """Test that get_test_summary returns None for non-test commands."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        mock_result = Mock()

        summary = manager.get_test_summary(mock_result)

        assert summary is None


class TestCompilationErrors:
    """Test compilation error handling."""

    def test_extract_failed_models(self, dummy_paths):
        """Test extraction of failed models from result."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        # Mock result with compilation error
        try:
            from dbt.contracts.results import RunStatus

            mock_result = Mock()
            mock_result.result = Mock()

            mock_node = Mock()
            mock_node.name = "failing_model"
            mock_node.original_file_path = "models/failing_model.sql"

            mock_run_result = Mock()
            mock_run_result.status = RunStatus.Error
            mock_run_result.message = "Syntax error: invalid SQL"
            mock_run_result.node = mock_node

            mock_result.result.results = [mock_run_result]

            failed_models = manager._extract_failed_models(mock_result)

            assert len(failed_models) == 1
            assert failed_models[0]["name"] == "failing_model"
            assert "Syntax error" in failed_models[0]["error_message"]
        except ImportError:
            pytest.skip("dbt.contracts.results not available")


class TestSelectionCriteria:
    """Test selection criteria string generation."""

    def test_selection_criteria_string_with_select(self, dummy_paths):
        """Test selection criteria string with select args."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
            select_args="model1 model2",
        )

        criteria = manager.get_selection_criteria_string()

        assert "select_args: model1 model2" in criteria

    def test_selection_criteria_string_no_selection(self, dummy_paths):
        """Test selection criteria string with no selection."""
        profiles_dir, project_dir = dummy_paths

        manager = DbtManager(
            verb="run",
            target="dev",
            profiles_dir=profiles_dir,
            project_dir=project_dir,
        )

        criteria = manager.get_selection_criteria_string()

        assert criteria == "no selection criteria"
