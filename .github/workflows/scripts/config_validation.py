from pathlib import Path

import yaml
from pydantic import ValidationError
from validation_schema.databricks_job_validation_schema import DbxJobsConfig
from validation_schema.source_validation_schema import SourcesConfig

file_name_to_schema = {
    "databricks_job.yml": DbxJobsConfig,
    # "source_ms_azure_api.yml": SourcesConfig,
    # "source_dna_static.yml": SourcesConfig
}


def _load_yml(yml_path: Path) -> dict:
    """
    Ready yml
        Parameters:
            yml_path (str): path to yml file
        Returns:
            yml_dict (dict): dictionary with yml content
    """
    with yml_path.open("r") as file:
        yml_dict = yaml.safe_load(file)

    return yml_dict


def _is_valid(file_name: str, yml_dict: dict) -> tuple[bool, str]:
    """
    Validate yml file
        Parameters:
            yml_path (str): path to yml file
        Returns:
            is_valid (bool): True if yml is valid, False otherwise
    """
    if file_name.startswith("source_"):
        schema = SourcesConfig

    else:
        schema = file_name_to_schema.get(file_name)

    if schema is None:
        return True, "Warning - No schema found for this file"
    try:
        schema(**yml_dict)
        return True, "Valid"
    except ValidationError as exc:
        return False, str(exc)


def main():
    """
    Use pydantic to validate the yml files in ./config/**
    """

    # List of all yml files in the root and sub folders
    config_paths = list(Path("config").glob("**/*.yml")) + list(
        Path("edp_auto_loader").glob("**/*.yml")
    )

    errors: list[str] = []

    for path in config_paths:
        yml_dict = _load_yml(path)

        is_valid, msg = _is_valid(path.name, yml_dict)
        if is_valid:
            print(f">>>{path}: {msg}")
        else:
            errors.append(f"\n\n >>>Error in {path}: {msg}")

    if errors:
        raise Exception("\n".join(errors))


if __name__ == "__main__":
    main()
