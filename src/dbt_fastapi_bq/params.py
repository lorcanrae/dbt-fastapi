from pathlib import Path

DBT_PROJECT_PATH = str(Path(__file__).parent.parent.parent.resolve() / "dbt_project")


if __name__ == "__main__":
    print(DBT_PROJECT_PATH)
