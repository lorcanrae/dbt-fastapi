import subprocess
import re
from fastapi import HTTPException, status


def execute_dbt_command(
    command: list[str],
    *,
    target: str | None = None,
    model: str | None = None,
) -> str:
    try:
        result: subprocess.CompletedProcess[str] = subprocess.run(
            command, capture_output=True, text=True, check=True
        )
        stdout: str = result.stdout

        # Handle empty model match (even though exit code is 0)
        if "does not match any enabled nodes" in stdout or "Nothing to do" in stdout:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Invalid dbt model selection",
                    "provided_model": model,
                    "message": stdout.strip(),
                },
            )

        return stdout

    except subprocess.CalledProcessError as e:
        error_str: str = e.stderr or e.stdout or "No error message captured."

        if "does not have a target named" in error_str:
            valid_targets: list[str] = re.findall(r"- (\w+)", error_str)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Invalid dbt target",
                    "provided_target": target,
                    "valid_targets": valid_targets,
                    "message": error_str.strip(),
                },
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "dbt command failed", "message": error_str.strip()},
        )
