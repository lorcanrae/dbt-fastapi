import subprocess
import os
import re
import shlex
from fastapi import status, HTTPException
from pathlib import Path

from dbt_fastapi_bq.params import PROJECT_ROOT


class DbtManager:
    """
    Manages the construction and execution of dbt CLI commands within a FastAPI app.
    """

    EXCLUDED_DIRS = [".venv", ".git", "__pycache__", ".pytest_cache", "logs"]

    def __init__(
        self,
        verb: str,
        target: str,
        select_args: str | None = None,
        exclude_args: str | None = None,
        selector_args: str | None = None,
        resource_type: str | None = None,
    ):
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
        self.select_args: str | None = self._shlex_quote_input(select_args)
        self.exclude_args: str | None = self._shlex_quote_input(exclude_args)
        self.selector_args: str | None = self._shlex_quote_input(selector_args)

        self.profiles_yaml_dir, self.dbt_project_yaml_dir = (
            self._get_dbt_conf_files_paths()
        )
        self.dbt_cli_command: list[str] = self._generate_dbt_command()

    def execute_dbt_command(self) -> str:
        """
        Run the dbt command synchronously and return its stdout output.

        Raises:
            HTTPException: If the dbt command fails or configuration is invalid.

        Returns:
            Sanitized stdout output from dbt CLI.
        """
        dbt_cli_command = self.dbt_cli_command

        try:
            result = subprocess.run(
                dbt_cli_command, capture_output=True, text=True, check=True
            )
            output = self.strip_ansi_codes(result.stdout)
        except subprocess.CalledProcessError as e:
            output = (
                self.strip_ansi_codes(e.stderr or e.stdout)
                or "No error message captured."
            )

            # Target error
            if "does not have a target named" in output:
                valid_targets: list[str] = re.findall(r"- (\w+)", output)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "Invalid dbt target",
                        "provided_target": self.target or "No target found",
                        "valid_targets": valid_targets,
                        "message": output,
                    },
                )

            # Fallback
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"error": "dbt command failed", "message": output},
            )

        # More error handling
        # Parse the output because dbt's CLI return codes are inconsistent
        # Why does an invalid target return non-zero, but an invalid model return zero?
        self._parse_dbt_command_stdout(output)

        return output

    async def async_execute_dbt_command(self):
        """
        Placeholder for future async execution support.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("Async dbt execution is not yet implemented.")

    @classmethod
    def execute_unsafe_dbt_command(cls, cli_command: list[str]) -> str:
        """
        Execute a raw dbt CLI command without argument sanitation.

        Args:
            cli_command: A list representing the full dbt CLI command.

        Raises:
            HTTPException: On execution failure or invalid configuration.

        Returns:
            Sanitized stdout output.
        """
        try:
            result = subprocess.run(
                cli_command, capture_output=True, text=True, check=True
            )
            output = cls.strip_ansi_codes(result.stdout)
        except subprocess.CalledProcessError as e:
            output = (
                cls.strip_ansi_codes(e.stderr or e.stdout)
                or "No error message captured."
            )

            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"error": "dbt command failed", "message": output},
            )

        # More error handling
        # Parse the output because dbt's CLI return codes are inconsistent
        # Why does an invalid target return non-zero, but an invalid model return zero?
        cls._parse_dbt_command_stdout(output)

        return output

    @classmethod
    async def async_execute_unsafe_dbt_command(cls, cli_command: list[str]) -> str:
        """
        Placeholder for future async execution support.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("Async dbt execution is not yet implemented.")

    # === Private Helpers ===

    def _generate_dbt_command(self) -> list[str]:
        """
        Construct the dbt CLI command based on instance attributes.

        Returns:
            A list representing the full dbt command to be executed.
        """
        dbt_cmd = [
            "dbt",
            self.verb,
            "--project-dir",
            self.dbt_project_yaml_dir,
            "--profiles-dir",
            self.profiles_yaml_dir,
        ]

        if self.select_args:
            dbt_cmd += ["--select", self.select_args]
        if self.exclude_args:
            dbt_cmd += ["--exclude", self.exclude_args]
        if self.selector_args:
            dbt_cmd += ["--selector", self.selector_args]

        dbt_cmd += ["--target", self.target]

        return dbt_cmd

    def _get_dbt_conf_files_paths(self):
        """
        Locate and validate paths for dbt_project.yml and profiles.yml.

        Returns:
            A tuple of (profiles_dir, project_dir) as strings.

        Raises:
            HTTPException: If config files are missing or duplicated.
        """

        dbt_project_dir = os.environ.get("DBT_PROJECT_DIR")
        dbt_profiles_dir = os.environ.get("DBT_PROFILES_DIR")

        if dbt_profiles_dir and dbt_project_dir:
            return dbt_profiles_dir, dbt_project_dir

        # Discover dirs
        dbt_project_yaml_dirs: list = []
        profiles_yaml_dirs: list = []
        selectors_yaml_dirs: list = []

        # Find the parent dirs
        for root, dirs, files in os.walk(PROJECT_ROOT):
            dirs[:] = [d for d in dirs if d not in DbtManager.EXCLUDED_DIRS]
            root_path = Path(root)

            if "dbt_project.yml" in files:
                dbt_project_yaml_dirs.append(root_path.resolve())
            if "profiles.yml" in files:
                profiles_yaml_dirs.append(root_path.resolve())
            if self.selector_args and "selectors.yml" in files:
                selectors_yaml_dirs.append(root_path.resolve())

        # Error handling
        self._dbt_conf_files_path_errors("dbt_project.yml", len(dbt_project_yaml_dirs))
        self._dbt_conf_files_path_errors("profiles.yml", len(profiles_yaml_dirs))

        # Only needs to exist if selector_args are passed
        if self.selector_args:
            self._dbt_conf_files_path_errors("selectors.yml", len(selectors_yaml_dirs))
            if selectors_yaml_dirs[0] != dbt_project_yaml_dirs[0]:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="'selectors.yml' not at same level as 'dbt_project.yml'",
                )

        return str(profiles_yaml_dirs[0]), str(dbt_project_yaml_dirs[0])

    # === Utilities ===

    @staticmethod
    def _shlex_quote_input(input: str) -> str:
        """
        Quote a CLI argument string using shlex for safety.

        Args:
            input: The raw CLI input string.

        Returns:
            A safely quoted string or None.
        """
        if input:
            return shlex.quote(input)
        return None

    @staticmethod
    def _parse_dbt_command_stdout(terminal_output):
        """
        Raise 400 errors for invalid dbt model selection based on output.

        Args:
            terminal_output: Raw output from dbt CLI.

        Raises:
            HTTPException: If CLI indicates no models were matched or run.
        """
        if (
            "does not match any enabled nodes" in terminal_output
            or "Nothing to do" in terminal_output
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Invalid dbt model selection",
                    "message": terminal_output,
                },
            )

    @staticmethod
    def _dbt_conf_files_path_errors(file_type: str, num_files_found: int):
        """
        Raise informative HTTP errors based on file discovery results.

        Args:
            file_type: The name of the file (e.g., dbt_project.yml).
            num_files_found: How many were discovered.

        Raises:
            HTTPException: If none or more than one copy of the file was found.
        """

        # File not found
        if num_files_found == 0:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"error": f"'{file_type}' not found."},
            )
        # More than one of a file found
        if num_files_found > 1:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"error": f"More than one '{file_type}' found."},
            )

    @staticmethod
    def strip_ansi_codes(text: str) -> str:
        """
        Strip ANSI escape sequences from terminal output.

        Args:
            text: Terminal string possibly containing ANSI codes.

        Returns:
            Cleaned string without formatting codes.
        """
        ansi_escape: re.Pattern[str] = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
        return ansi_escape.sub("", text)


if __name__ == "__main__":
    pass
