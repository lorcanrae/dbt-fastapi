import os
import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from dbt.cli.main import dbtRunner, dbtRunnerResult

from dbt_fastapi.params import PROJECT_ROOT
from dbt_fastapi.exceptions import (
    DbtFastApiError,
    translate_dbt_exception,
    create_model_selection_error,
    create_execution_failure_error,
    create_configuration_missing_error,
    create_configuration_duplicate_error,
    create_target_selection_error,
    create_compilation_error,
)


class DbtManager:
    """
    Manages the construction and execution of dbt commands using the dbt Python API.

    This class follows clean architecture principles by:
    - Using domain-specific exceptions instead of dbt's internal exceptions
    - Translating dbt errors into application-specific errors
    - Providing clear separation between dbt concerns and API concerns
    """

    EXCLUDED_DIRS = [
        ".venv",
        ".git",
        "__pycache__",
        ".pytest_cache",
        "logs",
        "dbt_internal_packages",
    ]

    def __init__(
        self,
        verb: str,
        target: str,
        select_args: str | None = None,
        exclude_args: str | None = None,
        selector_args: str | None = None,
        resource_type: str | None = None,
    ) -> None:
        """
        Initialize a DbtManager instance.

        Args:
            verb: The dbt command to run (e.g., "run", "build").
            target: The dbt target profile to use.
            select_args: Optional dbt --select argument.
            exclude_args: Optional dbt --exclude argument.
            selector_args: Optional dbt --selector argument.
            resource_type: Currently unused; reserved for future support.
        """
        self.verb: str = verb
        self.target: str = target
        self.select_args: list[str] = select_args.split() if select_args else []
        self.exclude_args: list[str] = exclude_args.split() if exclude_args else []
        self.selector_args: list[str] = selector_args.split() if selector_args else []

        # Raise custom exceptions if needed.
        try:
            self.profiles_yaml_dir, self.dbt_project_yaml_dir = (
                self._get_dbt_conf_files_paths()
            )
            self.dbt_cli_args: list[str] = self._generate_dbt_args()
            self.runner = dbtRunner()
        except DbtFastApiError as e:
            raise HTTPException(
                status_code=e.http_status_code,
                detail={"error": type(e).__name__, "message": e.message, **e.details},
            )

    def execute_dbt_command(self) -> str:
        """
        Run the dbt command using the Python API and return its output.

        Raises:
            HTTPException: If the dbt command fails or configuration is invalid.

        Returns:
            Formatted output from dbt execution.
        """
        try:
            # Execute using dbt Python API
            result: dbtRunnerResult = self.runner.invoke(self.dbt_cli_args)

            # Check for application-level errors
            self._validate_dbt_result(result)

            return result

        except DbtFastApiError as e:
            # Our custom exceptions - convert to HTTP exceptions
            raise HTTPException(
                status_code=e.http_status_code,
                detail={
                    "error": type(e).__name__,
                    "message": e.message,
                    **e.details,
                },
            )

        except Exception as dbt_exception:
            # Catch all
            context = {
                "target": self.target,
                "command": self.dbt_cli_args,
                "profiles_dir": self.profiles_yaml_dir,
                "project_dir": self.dbt_project_yaml_dir,
            }

            app_exception = translate_dbt_exception(dbt_exception, context)

            raise HTTPException(
                status_code=app_exception.http_status_code,
                detail={
                    "error": type(app_exception).__name__,
                    "message": app_exception.message,
                    **app_exception.details,
                },
            )

    async def async_execute_dbt_command(self) -> str:
        """
        Placeholder for future async execution support.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("Async dbt execution is not yet implemented.")

    @classmethod
    def execute_unsafe_dbt_command(cls, cli_command: list[str]) -> str:
        """
        Execute a raw dbt CLI command using the Python API.

        Args:
            cli_command: A list representing the full dbt CLI command.

        Raises:
            HTTPException: On execution failure or invalid configuration.

        Returns:
            Formatted output.
        """
        try:
            runner = dbtRunner()
            # Remove 'dbt' from the command as it's not needed for the API
            if cli_command[0] == "dbt":
                cli_command = cli_command[1:]

            result: dbtRunnerResult = runner.invoke(cli_command)

            if not result.success:
                raise create_execution_failure_error(cli_command)

            return result

        except DbtFastApiError as e:
            # Our custom exceptions
            raise HTTPException(
                status_code=e.http_status_code,
                detail={
                    "error": type(e).__name__,
                    "message": e.message,
                    **e.details,
                },
            )

        except Exception as dbt_exception:
            # Translate dbt exceptions
            context = {"command": cli_command}
            app_exception = translate_dbt_exception(dbt_exception, context)

            raise HTTPException(
                status_code=app_exception.http_status_code,
                detail={
                    "error": type(app_exception).__name__,
                    "message": app_exception.message,
                    **app_exception.details,
                },
            )

    @classmethod
    async def async_execute_unsafe_dbt_command(cls, cli_command: list[str]) -> str:
        """
        Placeholder for future async execution support.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("Async dbt execution is not yet implemented.")

    def get_list_nodes(self, result: dbtRunnerResult) -> list[str]:
        """
        Parse the dbt list command output into a list of nodes.

        Args:
            result: The dbtRunnerResult from executing 'dbt list'

        Returns:
            List of nodes as strings
        """

        if hasattr(result, "result") and result.result:
            if isinstance(result.result, list):
                return result.result
        return []

    # === Private Helpers ===

    def _generate_dbt_args(self) -> list[str]:
        """
        Construct the dbt command arguments for the Python API.

        Returns:
            A list representing the dbt command arguments.
        """
        # Start with the verb (run, test, build, etc.)
        dbt_args = [
            self.verb,
            "--project-dir",
            self.dbt_project_yaml_dir,
            "--profiles-dir",
            self.profiles_yaml_dir,
        ]

        # Add selection arguments
        if self.select_args:
            dbt_args.extend(["--select"] + self.select_args)
        if self.exclude_args:
            dbt_args.extend(["--exclude"] + self.exclude_args)
        if self.selector_args:
            dbt_args.extend(["--selector"] + self.selector_args)

        # Add target
        dbt_args.extend(["--target", self.target])

        return dbt_args

    def _get_dbt_conf_files_paths(self) -> tuple[str, str]:
        """
        Locate and validate paths for dbt_project.yml and profiles.yml.

        Returns:
            A tuple of (profiles_dir, project_dir) as strings.

        Raises:
            DbtConfigurationError: If config files are missing or duplicated.
        """
        dbt_project_dir = os.environ.get("DBT_PROJECT_DIR")
        dbt_profiles_dir = os.environ.get("DBT_PROFILES_DIR")

        if dbt_profiles_dir and dbt_project_dir:
            return dbt_profiles_dir, dbt_project_dir

        # Discover dirs
        dbt_project_yaml_dirs: list[Path] = []
        profiles_yaml_dirs: list[Path] = []
        selectors_yaml_dirs: list[Path] = []
        search_paths: list[str] = []

        # Find the parent dirs
        for root, dirs, files in os.walk(PROJECT_ROOT):
            dirs[:] = [d for d in dirs if d not in DbtManager.EXCLUDED_DIRS]
            root_path = Path(root)
            search_paths.append(str(root_path))

            if "dbt_project.yml" in files:
                dbt_project_yaml_dirs.append(root_path.resolve())
            if "profiles.yml" in files:
                profiles_yaml_dirs.append(root_path.resolve())
            if self.selector_args and "selectors.yml" in files:
                selectors_yaml_dirs.append(root_path.resolve())

        # Validate configuration files using our custom exceptions
        self._validate_config_files(
            "dbt_project.yml", dbt_project_yaml_dirs, search_paths
        )
        self._validate_config_files("profiles.yml", profiles_yaml_dirs, search_paths)

        # Validate selectors.yml if needed
        if self.selector_args:
            self._validate_config_files(
                "selectors.yml", selectors_yaml_dirs, search_paths
            )

            # Ensure selectors.yml is at same level as dbt_project.yml
            if selectors_yaml_dirs[0] != dbt_project_yaml_dirs[0]:
                raise create_configuration_missing_error(
                    "selectors.yml", [str(dbt_project_yaml_dirs[0])]
                )

        return str(profiles_yaml_dirs[0]), str(dbt_project_yaml_dirs[0])

    def _validate_dbt_result(self, result: dbtRunnerResult) -> None:
        """
        Validate dbt execution result and raise appropriate exceptions.

        Args:
            result: The dbtRunnerResult object.

        Raises:
            DbtExecutionError: When dbt execution fails.
            DbtModelSelectionError: When no models match selection criteria.
        """

        # Successful execution but operation failure
        if not result.success:
            # Check for valid target
            if result.exception and "does not have a target named" in str(
                result.exception
            ):
                valid_targets: list[str] = re.findall(r"- (\w+)", str(result.exception))
                raise create_target_selection_error(self.target, valid_targets)

            # Check for SQL syntax compilation errors
            if self.verb != "list" and hasattr(result, "result") and result.result:
                failed_models = self._extract_failed_models(result)
                if failed_models:
                    raise create_compilation_error(failed_models)

            # Generic
            raise create_execution_failure_error(self.dbt_cli_args)

        # Successful execution, but has other issues
        # Model selection errors
        if self.verb != "list" and result.success and hasattr(result, "result"):
            if hasattr(result.result, "results") and len(result.result.results) == 0:
                selection_criteria = self._get_selection_criteria_string()
                raise create_model_selection_error(selection_criteria)

    def _get_selection_criteria_string(self) -> str:
        """
        Get a string representation of the current selection criteria.

        Returns:
            A string describing the selection criteria used.
        """
        criteria_parts = []

        if self.select_args:
            criteria_parts.append(f"select_args: {' '.join(self.select_args)}")
        if self.exclude_args:
            criteria_parts.append(f"exclude_args: {' '.join(self.exclude_args)}")
        if self.selector_args:
            criteria_parts.append(f"selector_args: {' '.join(self.selector_args)}")

        return ", ".join(criteria_parts) if criteria_parts else "no selection criteria"

    def _extract_failed_models(self, result: dbtRunnerResult) -> list[dict[str, Any]]:
        """
        Extract failed models with compilation errors from dbt result.

        Args:
            result: The dbtRunnerResult object.

        Returns:
            List of dictionaries containing failed model information.
        """
        failed_models: list[dict[str, Any]] = []

        if not hasattr(result.result, "results"):
            return failed_models

        try:
            from dbt.contracts.results import RunStatus
        except ImportError:
            return failed_models

        for run_result in result.result.results:
            if hasattr(run_result, "status") and run_result.status == RunStatus.Error:
                model_info = {
                    "name": getattr(run_result.node, "name", "unknown"),
                    "path": getattr(run_result.node, "original_file_path", "unknown"),
                    "error_message": run_result.message
                    or "Unknown compination message",
                }

                if run_result.message:
                    error_lines = run_result.message.split("\n")
                    for line in error_lines:
                        if "Syntax error:" in line or "Error:" in line:
                            model_info["syntax_error"] = line.strip()
                            break

                failed_models.append(model_info)

        return failed_models

    def _validate_config_files(
        self, file_type: str, found_paths: list[Path], search_paths: list[str]
    ) -> None:
        """
        Validate that exactly one config file of the given type was found.

        Args:
            file_type: The name of the file (e.g., dbt_project.yml).
            found_paths: List of paths where the file was found.
            search_paths: List of all paths that were searched.

        Raises:
            DbtConfigurationError: If none or more than one copy found.
        """
        if len(found_paths) == 0:
            raise create_configuration_missing_error(file_type, search_paths)
        elif len(found_paths) > 1:
            raise create_configuration_duplicate_error(
                file_type, [str(path) for path in found_paths]
            )


if __name__ == "__main__":
    pass
