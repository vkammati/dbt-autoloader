import os
import sys
from importlib import resources
from typing import Dict

import yaml

sys.path.append(os.path.abspath(".."))


def get_missing_required_keys(
    dictionary_to_check: dict, required_keys: list, exclude_empty_values: bool = True
) -> list:
    """
    Check if all required_keys are present in the provided dictionary
    """

    # Get a list of relevant keys
    if exclude_empty_values:
        existing_keys = [k for k, v in dictionary_to_check.items() if v is not None]
    else:
        existing_keys = list(dictionary_to_check.keys())

    # Compare the list using sets and find missing keys
    missing_required_fields = set(required_keys) - set(existing_keys)

    # Return result
    return list(missing_required_fields)


def strip_slashes(
    path: str, strip_leading_slash: bool = True, strip_trailing_slash: bool = True
) -> str:
    """
    Strip any leading and/or trailing slashes from the provided 'path' and
    return the result
    """

    if strip_leading_slash and path[0] == "/":
        path = path[1:]
    if strip_trailing_slash and path[-1] == "/":
        path = path[:-1]

    return path


def load_yml_files(package: str, directory: str) -> Dict:
    """
    Loads default delta_table_properties and all the source ymls as python dictionaries
    """

    yml_data = {
        "auto_loader": {
            "auto_loader": {
                "delta_table_properties": {
                    "minReaderVersion": 2,
                    "minWriterVersion": 5,
                    "columnMapping.mode": "name",
                    "enableChangeDataFeed": "true",
                }
            }
        }
    }
    dir_path = resources.files(package).joinpath(directory)
    for file in dir_path.iterdir():
        if file.suffix == ".yml":
            with open(file, "r") as yml_file:
                yml_data[file.stem] = yaml.safe_load(yml_file)
    return yml_data


def merge_config(config_ymls) -> Dict:
    """
    Create one dictionary with all auto loader + sources configuration
    """
    config = config_ymls["auto_loader"]
    config_ymls.pop("auto_loader")
    config["auto_loader"]["sources"] = []
    sources_name = []

    for v in config_ymls.values():
        for i in v["sources"]:
            if i["name"] in sources_name:
                raise Exception(
                    f"Source name >>{i['name']}<< was configured multiple times"
                )
            else:
                sources_name.append(i["name"])
                config["auto_loader"]["sources"].append(i)

    return config
