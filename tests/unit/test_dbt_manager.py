import pytest
import subprocess
from fastapi import HTTPException
from dbt_fastapi.dbt_manager import DbtManager
from dbt_fastapi.schemas.dbt_schema import DbtRunTestCompileSeedSnapshotDocs


# === CLI Construction ===


def test_generate_dbt_command_from_env(monkeypatch, dummy_paths):
    profiles_dir, project_dir = dummy_paths
    monkeypatch.setenv("DBT_PROFILES_DIR", profiles_dir)
    monkeypatch.setenv("DBT_PROJECT_DIR", project_dir)

    # Case 1: single-word model
    manager = DbtManager(verb="run", target="dev", select_args="model")
    assert manager.dbt_cli_command == [
        "dbt",
        "run",
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
        "--select",
        "model",
        "--target",
        "dev",
    ]

    # Case 2: multi-word model (quoted)
    manager = DbtManager(verb="run", target="dev", select_args="model model")
    assert manager.dbt_cli_command == [
        "dbt",
        "run",
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
        "--select",
        "'model model'",
        "--target",
        "dev",
    ]


# === Subprocess Execution ===


def test_execute_dbt_command_success(monkeypatch, dummy_paths, mocker):
    profiles_dir, project_dir = dummy_paths
    monkeypatch.setenv("DBT_PROFILES_DIR", profiles_dir)
    monkeypatch.setenv("DBT_PROJECT_DIR", project_dir)

    mocker.patch("subprocess.run").return_value.stdout = "Success message"
    manager = DbtManager(verb="run", target="dev")
    output = manager.execute_dbt_command()

    assert "Success" in output


def test_execute_unsafe_dbt_command_strips_ansi_and_returns_output(mocker):
    mock_run = mocker.patch("subprocess.run")
    mock_run.return_value.stdout = "\x1b[31mUnsafe\x1b[0m command executed"
    mock_run.return_value.returncode = 0

    output = DbtManager.execute_unsafe_dbt_command(["dbt", "run"])

    # Verify ANSI codes are stripped
    assert "Unsafe command executed" == output
    assert "\x1b" not in output

    # Verify the right command is passed
    mock_run.assert_called_once_with(
        ["dbt", "run"], capture_output=True, text=True, check=True
    )


def test_execute_unsafe_dbt_command_raises_on_failure(mocker):
    mocker.patch(
        "subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "cmd", stderr="some error"),
    )

    with pytest.raises(HTTPException) as exc:
        DbtManager.execute_unsafe_dbt_command(["dbt", "run"])

    assert exc.value.status_code == 500
    assert "some error" in exc.value.detail["message"]


# === Error Handling ===


def test_execute_dbt_command_invalid_target(monkeypatch, dummy_paths, mocker):
    profiles_dir, project_dir = dummy_paths
    monkeypatch.setenv("DBT_PROFILES_DIR", profiles_dir)
    monkeypatch.setenv("DBT_PROJECT_DIR", project_dir)

    stderr = "ERROR: The profile does not have a target named bad_target\n- dev\n- prod"
    mock_run = mocker.patch(
        "subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "cmd", stderr=stderr),
    )

    manager = DbtManager(verb="run", target="bad_target")

    with pytest.raises(HTTPException) as exc:
        manager.execute_dbt_command()

    assert mock_run.call_count == 1
    assert exc.value.status_code == 400

    detail = exc.value.detail

    assert "valid_targets" in detail
    assert "provided_target" in detail
    assert "message" in detail

    assert detail["provided_target"] == "bad_target"
    assert detail["valid_targets"] == ["dev", "prod"]
    assert "does not have a target named" in detail["message"]


def test_execute_dbt_command_invalid_model(monkeypatch, dummy_paths, mocker):
    profiles_dir, project_dir = dummy_paths
    monkeypatch.setenv("DBT_PROFILES_DIR", profiles_dir)
    monkeypatch.setenv("DBT_PROJECT_DIR", project_dir)

    mocker.patch("subprocess.run").return_value.stdout = "Nothing to do"
    manager = DbtManager(verb="run", target="dev")

    with pytest.raises(HTTPException) as exc:
        manager.execute_dbt_command()

    assert exc.value.status_code == 400
    assert "Invalid dbt model selection" in str(exc.value.detail)


def test_execute_unsafe_dbt_command_failure(mocker):
    mocker.patch(
        "subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "cmd", stderr="failure"),
    )

    with pytest.raises(HTTPException) as exc:
        DbtManager.execute_unsafe_dbt_command(["dbt", "run"])

    assert exc.value.status_code == 500


def test_mutually_exclusive_select_and_selector():
    with pytest.raises(ValueError) as exc:
        DbtRunTestCompileSeedSnapshotDocs(
            target="dev", select_args="model", selector_args="selector"
        )
    assert "mutually exclusive" in str(exc.value)


# === Static / Utility ===


def test_shlex_quote_input():
    assert DbtManager._shlex_quote_input("model_name") == "model_name"
    assert (
        DbtManager._shlex_quote_input("model_name model_name")
        == "'model_name model_name'"
    )
    assert DbtManager._shlex_quote_input(None) is None


def test_strip_ansi_codes():
    raw = "\x1b[31mError:\x1b[0m Something went wrong"
    clean = DbtManager.strip_ansi_codes(raw)
    assert "Error:" in clean
    assert "\x1b" not in clean


def test_parse_dbt_command_stdout_invalid_model():
    with pytest.raises(HTTPException) as exc:
        DbtManager._parse_dbt_command_stdout("Nothing to do")

    assert exc.value.status_code == 400


# === Conf File Discovery ===


def test_multiple_profiles_yml_raises(monkeypatch, tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    (tmp_path / "a" / "profiles.yml").write_text("name: profiles")
    (tmp_path / "b" / "profiles.yml").write_text("name: profiles")
    (tmp_path / "dbt_project.yml").write_text("name: dbt_project")

    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    monkeypatch.setattr("dbt_fastapi.dbt_manager.PROJECT_ROOT", tmp_path)

    with pytest.raises(HTTPException) as exc:
        DbtManager(verb="run", target="dev")

    assert "profiles.yml" in str(exc.value.detail)


def test_dbt_paths_fallback_to_project_discovery(monkeypatch, tmp_path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: dbt_project")
    (project_dir / "profiles.yml").write_text("name: profiles")

    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
    monkeypatch.setattr("dbt_fastapi.dbt_manager.PROJECT_ROOT", tmp_path)

    manager = DbtManager(verb="run", target="dev")

    assert manager.dbt_project_yaml_dir == str(project_dir)
    assert manager.profiles_yaml_dir == str(project_dir)
