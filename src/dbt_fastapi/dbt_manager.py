import re
from typing import Any
import json

from dbt.cli.main import dbtRunner, dbtRunnerResult

from dbt_fastapi.exceptions import (
    DbtFastApiError,
    DbtTargetError,
    DbtExecutionError,
    translate_dbt_exception,
    create_compilation_error,
)
from dbt_fastapi.schemas.dbt_schema import DbtTestResult, ResponseTestStatus


class DbtManager:
    """
    Manages the construction and execution of dbt commands using the dbt Python API.

    Guiding principles:
    - Using domain-specific exceptions instead of dbt's internal exceptions
    - De-coupled from the HTTP layer
    - Accept configuration via dependency injection
    - Providing clear separation between dbt concerns and API concerns

    All exceptions raised from this class are DbtFastApiError or its subclasses,
    which are then converted to HTTP responses by FastAPI exception handlers.
    """

    def __init__(
        self,
        verb: str,
        target: str,
        profiles_dir: str,
        project_dir: str,
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

        Raises:
            DbtConfigurationError: If configuration files are missing or invalid
            DbtFastApiError: For other initialization errors
        """
        self.verb: str = verb
        self.target: str = target
        self.select_args: list[str] = select_args.split() if select_args else []
        self.exclude_args: list[str] = exclude_args.split() if exclude_args else []
        self.selector_args: list[str] = selector_args.split() if selector_args else []

        self.profiles_yaml_dir: str = profiles_dir
        self.dbt_project_yaml_dir: str = project_dir

        # Build CLI args
        self.dbt_cli_args: list[str] = self._generate_dbt_args()

        # Initialize dbt runner
        self.runner = dbtRunner()

    def execute_dbt_command(self) -> dbtRunnerResult:
        """
        Run the dbt command using the Python API and return its output.

        Raises:
            DbtTargetError: If an invalid target is specified
            DbtCompilationError: If models fail to compile
            DbtExecutionError: If dbt command execution fails
            DbtConfigurationError: If configuration is invalid
            DbtInternalError: For unexpected errors

        Returns:
            dbtRunnerResult from dbt execution
        """
        try:
            # Execute using dbt Python API
            result: dbtRunnerResult = self.runner.invoke(self.dbt_cli_args)

            # Check for application-level errors
            self._validate_dbt_result(result)

            return result

        except DbtFastApiError:
            # Custom error
            # FastAPI exception handler will convert to HTTP response
            raise

        except Exception as dbt_exception:
            # Translate dbt's internal exceptions to domain exceptions
            context = {
                "target": self.target,
                "command": self.dbt_cli_args,
                "profiles_dir": self.profiles_yaml_dir,
                "project_dir": self.dbt_project_yaml_dir,
            }

            app_exception = translate_dbt_exception(dbt_exception, context)
            raise app_exception

    async def async_execute_dbt_command(self) -> dbtRunnerResult:
        """
        Placeholder for future async execution support.

        Raises:
            NotImplementedError: This method is not yet implemented
        """
        raise NotImplementedError("Async dbt execution is not yet implemented.")

    @classmethod
    def execute_unsafe_dbt_command(cls, cli_command: list[str]) -> dbtRunnerResult:
        """
        Execute a raw dbt CLI command using the Python API.

        Args:
            cli_command: A list representing the full dbt CLI command

        Raises:
            DbtExecutionError: On execution failure
            DbtConfigurationError: On invalid configuration
            DbtInternalError: Unexpected errors

        Returns:
            dbtRunnerResult from dbt execution
        """
        try:
            runner = dbtRunner()
            # Remove 'dbt' from the command as it's not needed for the API
            if cli_command[0] == "dbt":
                cli_command = cli_command[1:]

            result: dbtRunnerResult = runner.invoke(cli_command)

            if not result.success:
                raise DbtExecutionError(cli_command)

            return result

        except DbtFastApiError:
            # Custom error
            # FastAPI exception handler will convert to HTTP response
            raise

        except Exception as dbt_exception:
            # Translate dbt's internal exceptions to domain exceptions
            context = {"command": cli_command}
            app_exception = translate_dbt_exception(dbt_exception, context)
            raise app_exception

    @classmethod
    async def async_execute_unsafe_dbt_command(
        cls, cli_command: list[str]
    ) -> dbtRunnerResult:
        """
        Placeholder for future async execution support.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("Async dbt execution is not yet implemented.")

    def get_nodes_from_result(self, result: dbtRunnerResult) -> list[dict[str, Any]]:
        """
        Extract node names from any dbt command result.

        This works for both 'list' commands and execution commands like 'run', 'test', 'build'.

        Args:
            result: dbtRunnerResult from executing a dbt command

        Returns:
            List of node dictionaries with metadata
        """
        nodes: list[dict[str]] = []

        # Handle list command results (returns a list of strings directly)
        if hasattr(result, "result") and result.result:
            # for dbt list
            if isinstance(result.result, list):
                new_result = [json.loads(json_string) for json_string in result.result]

                for node in new_result:
                    unique_id = node["unique_id"]
                    resource_type = node["resource_type"]
                    fqn = node["alias"]  # good enough
                    depends_on = node["depends_on"].get("nodes")

                    node_data = {
                        "unique_id": unique_id,
                        "fqn": fqn,
                        "resource_type": resource_type,
                        "depends_on": depends_on,
                    }

                    nodes.append(node_data)

            # literally every other command
            elif hasattr(result.result, "results"):
                for run_result in result.result.results:
                    if hasattr(run_result, "node"):
                        node = run_result.node

                        unique_id = getattr(node, "unique_id", "unknown")
                        resource_type = str(getattr(node, "resource_type", "None"))

                        # get the fqn for the node
                        fqn = None
                        if hasattr(node, "fqn") and node.fqn:
                            if isinstance(node.fqn, list):
                                fqn = ".".join(node.fqn)
                            else:
                                fqn = str(node.fqn)

                        # get the upstream node dependencies
                        depends_on = []

                        if hasattr(node, "depends_on") and hasattr(
                            node.depends_on, "nodes"
                        ):
                            depends_on += node.depends_on.nodes

                        node_data = {
                            "unique_id": unique_id,
                            "fqn": fqn,
                            "resource_type": resource_type,
                            "depends_on": depends_on,
                        }

                        # Add test result details for test/build commands
                        if (
                            self.verb == "test" or self.verb == "build"
                        ) and resource_type == "test":
                            test_result = self._extract_test_result_from_run_result(
                                run_result
                            )
                            if test_result:
                                node_data["test_result"] = test_result.model_dump()

                        nodes.append(node_data)

        return nodes

    def get_test_summary(self, result: dbtRunnerResult) -> dict[str, int] | None:
        """
        Extract test summary statistics from a dbt test command result.

        Args:
            result: dbtRunnerResult from executing a dbt test command

        Returns:
            Dictionary with test counts by status, or None if not a test command
        """
        if self.verb not in ["test", "build"]:
            return None

        summary = {
            "total": 0,
            "passed": 0,
            "warned": 0,
            "failed": 0,
            "errored": 0,
            "skipped": 0,
        }

        if not hasattr(result, "result") or not hasattr(result.result, "results"):
            return summary

        try:
            from dbt.contracts.results import TestStatus
        except ImportError:
            return summary

        for run_result in result.result.results:
            if (
                hasattr(run_result, "node")
                and str(getattr(run_result.node, "resource_type", "")) == "test"
            ):
                summary["total"] += 1

                status = getattr(run_result, "status", None)

                match status:
                    case TestStatus.Pass:
                        summary["passed"] += 1
                    case TestStatus.Warn:
                        summary["warned"] += 1
                    case TestStatus.Skipped:
                        summary["skipped"] += 1
                    case TestStatus.Fail:
                        summary["failed"] += 1
                    case TestStatus.Error:
                        summary["errored"] += 1
                    case _:
                        # handle unexpected
                        summary["errored"] += 1

        return summary

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

        if self.verb == "list":
            dbt_args += ["--output", "json"]

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
                raise DbtTargetError(
                    provided_target=self.target, valid_targets=valid_targets
                )

            # Check for SQL syntax compilation errors
            if self.verb != "list" and hasattr(result, "result") and result.result:
                failed_models = self._extract_failed_models(result)
                if failed_models:
                    raise create_compilation_error(failed_models)

            # Early return for tests to not raise an exception
            if (self.verb == "test" or self.verb == "build") and hasattr(
                result.result, "results"
            ):
                return

            # Generic
            raise DbtExecutionError(self.dbt_cli_args)

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

    def _extract_test_result_from_run_result(
        self, run_result: Any
    ) -> DbtTestResult | None:
        """
        Extract test result details from a single dbt run result.

        Args:
            run_result: A single run result from dbt execution

        Returns:
            DbtTestResult object with test execution details, or None if extraction fails
        """
        try:
            from dbt.contracts.results import TestStatus
        except ImportError:
            return None

        if not hasattr(run_result, "node") or not hasattr(run_result, "status"):
            return None

        # Map dbt status to enum
        status_mapping = {
            TestStatus.Pass: ResponseTestStatus.PASS,
            TestStatus.Error: ResponseTestStatus.ERROR,
            TestStatus.Fail: ResponseTestStatus.FAIL,
            TestStatus.Skipped: ResponseTestStatus.SKIP,
        }

        dbt_status = getattr(run_result, "status", None)
        api_status = status_mapping.get(dbt_status, ResponseTestStatus.ERROR)
        node = run_result.node
        message = (
            run_result.message
            if api_status in [ResponseTestStatus.FAIL, ResponseTestStatus.ERROR]
            else None
        )
        failures = (
            getattr(run_result, "failures", None)
            if api_status == ResponseTestStatus.FAIL
            else None
        )

        return DbtTestResult(
            unique_id=getattr(node, "unique_id", "unknown"),
            name=getattr(node, "name", "unknown"),
            status=api_status,
            execution_time=getattr(run_result, "execution_time", None),
            message=message,
            failures=failures,
        )


if __name__ == "__main__":
    pass
