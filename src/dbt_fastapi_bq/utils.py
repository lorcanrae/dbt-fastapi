import subprocess
import re
from fastapi import status, HTTPException

from dbt_fastapi_bq.models.dbt_models import (
    DbtCommandRequest,
    DbtModelSelectionError,
    DbtTargetValidationError,
    BaseDbtError,
)
from dbt_fastapi_bq.exceptions import DbtCommandException
from dbt_fastapi_bq.params import DBT_PROJECT_PATH


### dbt Utils
def generate_dbt_command(command: str, payload: str) -> list[str]:
    dbt_cmd: list[str] = ["dbt", command, "--project-dir", DBT_PROJECT_PATH]

    if payload.select_model:
        select_arg = payload.select_model
        if payload.upstream:
            select_arg = f"+{select_arg}"
        if payload.downstream:
            select_arg = f"{select_arg}+"
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
            raise DbtCommandException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=DbtTargetValidationError(
                    error="Invalid dbt target",
                    provided_target=payload.target,
                    valid_targets=valid_targets,
                    message=output,
                ),
            )

        raise DbtCommandException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=BaseDbtError(
                error="dbt command failed",
                message=output,
            ),
        )

    # Parse the output because dbt's cli return codes are annoying.
    # Why does an invalid target return non-zero, but an invalid model return zero?
    parse_dbt_command_stdout(output, model=payload.select_model, target=payload.target)

    return output


def parse_dbt_command_stdout(stdout: str, model, target):
    if "does not match any enabled nodes" in stdout or "Nothing to do" in stdout:
        raise DbtCommandException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=DbtModelSelectionError(
                error="Invalid dbt model selection",
                provided_model=model,
                message=stdout,
            ),
        )


### API Utils
def validate_dbt_command_request(payload: DbtCommandRequest) -> DbtCommandRequest:
    if (payload.upstream or payload.downstream) and not payload.select_model:
        raise HTTPException(
            status_code=400,
            detail="A valid 'model' parameter must be provided if 'select_prefix' or 'select_suffix' is True.",
        )
    return payload


def strip_ansi_codes(text: str) -> str:
    ansi_escape: re.Pattern[str] = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", text)
