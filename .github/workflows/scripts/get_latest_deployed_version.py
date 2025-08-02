"""
This script is used to get (output is printed) the latest deployed version of a wheel
"""

import argparse

from helpers.azure.token import get_access_token
from helpers.databricks.auth import get_dbx_http_header
from helpers.databricks.workspace import create_folder_if_not_exists
from helpers.wheel import get_wheels_from_workspace, get_workspace_wheel_path

parser = argparse.ArgumentParser(description="Get latest deployed version")

parser.add_argument("--host", help="Databricks host.", type=str, required=True)
parser.add_argument("--wheel-name", help="name of the wheel", type=str, required=True)

args = parser.parse_args()


def get_latest_version() -> str:
    # Get ad token token and use it to build the api header
    access_token = get_access_token()
    http_header = get_dbx_http_header(access_token)

    # Get folder path for wheel. Create it if it does not exists.
    folder_path = get_workspace_wheel_path(args.wheel_name)
    create_folder_if_not_exists(path=folder_path, host=args.host, http_header=http_header)

    # Get a list of wheels
    list_of_wheels = get_wheels_from_workspace(
        path=folder_path, host=args.host, http_header=http_header
    )

    # If there are no wheels, print an empty string an exit
    if not list_of_wheels:
        return ""

    # get max wheel
    max_wheel = max(list_of_wheels, key=lambda x: x["created_at"])
    # output version
    return str(max_wheel["version"])


if __name__ == "__main__":
    version = get_latest_version()
    print(version)
