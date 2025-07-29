import subprocess
import re
from fastapi import status, HTTPException

from dbt_fastapi_bq.schemas.dbt_command import DbtCommandRequest
from dbt_fastapi_bq.params import DBT_PROJECT_PATH


### dbt Utils
def generate_dbt_command(command: str, payload: DbtCommandRequest) -> list[str]:
    dbt_cmd: list[str] = ["dbt", command, "--project-dir", DBT_PROJECT_PATH]

    def _build_selector_arg(model: str, upstream: bool, downstream: bool) -> str:
        arg = model
        if upstream:
            arg = f"+{arg}"
        if downstream:
            arg = f"{arg}+"
        return arg

    if payload.select_model:
        select_arg = _build_selector_arg(
            payload.select_model, payload.select_upstream, payload.select_downstream
        )
        dbt_cmd += ["--select", select_arg]

    if payload.exclude_model:
        select_arg = _build_selector_arg(
            payload.exclude_model, payload.exclude_upstream, payload.exclude_downstream
        )
        dbt_cmd += ["--select", select_arg]

    dbt_cmd += ["--target", payload.target]

    return dbt_cmd


def execute_dbt_command(
    command: list[str],
    *,
    payload: DbtCommandRequest,
) -> str:
    # Try to run the dbt CLI command
    try:
        result: subprocess.CompletedProcess[str] = subprocess.run(
            command, capture_output=True, text=True, check=True
        )
        output: str = strip_ansi_codes(result.stdout)
    except subprocess.CalledProcessError as e:
        std = e.stderr or e.stdout
        output: str = strip_ansi_codes(std) or "No error message captured."

        if "does not have a target named" in output:
            valid_targets: list[str] = re.findall(r"- (\w+)", output)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Invalid dbt target",
                    "message": output,
                    "provided_target": payload.target,
                    "valid_target": valid_targets,
                },
            )

        # Generic fallback
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "dbt command failed", "message": output},
        )

    # Parse the output because dbt's cli return codes are annoying.
    # Why does an invalid target return non-zero, but an invalid model return zero?
    parse_dbt_command_stdout(output, model=payload.select_model, target=payload.target)

    return output


def parse_dbt_command_stdout(stdout: str, model, target):
    # Start with easy to catch errors
    if "does not match any enabled nodes" in stdout or "Nothing to do" in stdout:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Invalid dbt model selection",
                "provided_model": model,
                "message": stdout,
            },
        )
    # then parse the output into useable metadata


### API Utils
def strip_ansi_codes(text: str) -> str:
    ansi_escape: re.Pattern[str] = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", text)
