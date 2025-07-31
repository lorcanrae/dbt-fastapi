import pytest
from fastapi import status, HTTPException
from fastapi.testclient import TestClient
from dbt_fastapi.dbt_manager import DbtManager

COMMANDS = ["run", "test", "build"]


@pytest.mark.parametrize("command", COMMANDS)
def test_dbt_command_success(client: TestClient, mocker, command: str):
    mocker.patch.object(
        DbtManager, "_get_dbt_conf_files_paths", return_value=("profiles", "project")
    )
    mocker.patch.object(
        DbtManager, "execute_dbt_command", return_value=f"{command} completed"
    )

    response = client.post(f"/dbt/{command}", json={"target": "dev"})

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == "success"
    assert f"{command} completed" in response.json()["output"]


@pytest.mark.parametrize("command", COMMANDS)
def test_dbt_command_invalid_input(client: TestClient, command: str):
    payload = {
        "target": "dev",
        "select_args": "model_x",
        "selector_args": "tag:nightly",  # mutually exclusive
    }

    response = client.post(f"/dbt/{command}", json=payload)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert "mutually exclusive" in response.text


@pytest.mark.parametrize("command", COMMANDS)
def test_dbt_command_conf_file_error(client: TestClient, mocker, command: str):
    mocker.patch.object(
        DbtManager,
        "_get_dbt_conf_files_paths",
        side_effect=HTTPException(
            status_code=500, detail=f"{command}: config not found"
        ),
    )

    response = client.post(f"/dbt/{command}", json={"target": "dev"})

    assert response.status_code == 500
    assert f"{command}: config not found" in response.text


@pytest.mark.parametrize("command", COMMANDS)
def test_dbt_command_exec_failure(client: TestClient, mocker, command: str):
    mocker.patch.object(
        DbtManager, "_get_dbt_conf_files_paths", return_value=("profiles", "project")
    )
    mocker.patch.object(
        DbtManager,
        "execute_dbt_command",
        side_effect=HTTPException(status_code=400, detail=f"{command}: model invalid"),
    )

    response = client.post(f"/dbt/{command}", json={"target": "dev"})

    assert response.status_code == 400
    assert f"{command}: model invalid" in response.text
