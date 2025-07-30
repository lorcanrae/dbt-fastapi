import subprocess
import os
import re
from fastapi import status, HTTPException
from pathlib import Path

from dbt_fastapi_bq.params import PROJECT_ROOT


class DbtManager:
    EXCLUDED_DIRS = [".venv", ".git", "__pycache__", ".pytest_cache", "logs"]

    def __init__(
        self,
        verb: str,
        target: str,
        select_args: str = None,
        exclude_args: str = None,
        selector_args: str = None,
    ):
        self.verb: str = verb
        self.target: str = target
        self.select_args: str = select_args
        self.exclude_args: str = exclude_args
        self.selector_args: str = selector_args

        self.profiles_yaml_dir, self.dbt_project_yaml_dir = (
            self._get_dbt_conf_files_paths()
        )
        self.dbt_cmd: list[str] = self._generate_dbt_command()

    def _generate_dbt_command(self):
        dbt_cmd: list[str] = [
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

    def execute_dbt_command(self):
        try:
            result: subprocess.CompletedProcess[str] = subprocess.run(
                self.dbt_cmd, capture_output=True, text=True, check=True
            )
            output: str = self._strip_ansi_codes(result.stdout)
        except subprocess.CalledProcessError as e:
            output: str = (
                self._strip_ansi_codes(e.stderr or e.stdout)
                or "No error message captured."
            )

            # Target error
            if "does not have a target named" in output:
                valid_targets: list[str] = re.findall(r"- (\w+)", output)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "Invalid dbt target",
                        "provided_target": self.target,
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
        pass

    def _parse_dbt_command_stdout(self, terminal_output):
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

    def _get_dbt_conf_files_paths(self):
        # Try to read from env vars
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

    def _dbt_conf_files_path_errors(self, file_type: str, num_files_found: int):
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

    def _strip_ansi_codes(self, text: str) -> str:
        ansi_escape: re.Pattern[str] = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
        return ansi_escape.sub("", text)


# if __name__ == "__main__":
#     manager = DbtManager(verb="run", target="dev")

#     print(f"dbt_project.yml found in {manager.dbt_project_path}")
#     print(f"profiles.yml found in {manager.dbt_profiles_path}")
#     print(f"selectors.yml found in {manager.dbt_profiles_path}")
