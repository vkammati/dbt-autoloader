"""
This script is used to cleanup a workspace and remove obsolete wheels
"""

import argparse
from time import sleep

from helpers.azure.token import get_access_token
from helpers.databricks.auth import get_dbx_http_header
from helpers.databricks.workspace import create_folder_if_not_exists, delete_path
from helpers.wheel import (
    get_wheels_from_workspace,
    get_wheels_to_delete,
    get_workspace_wheel_path,
)

parser = argparse.ArgumentParser(description="Cleanup workspace")

parser.add_argument("--host", help="Databricks host.", type=str, required=True)
parser.add_argument("--wheel-name", help="name of the wheel", type=str, required=True)
parser.add_argument(
    "--nr-of-wheels-to-keep",
    help="Max number of wheels to keep in the workspace. Others will be deleted.",
    type=int,
    required=True,
)
parser.add_argument(
    "--nr-of-days-to-keep-wheel",
    help="Number of days to keep old wheels in the workspace. Older will be deleted.",
    type=int,
    required=True,
)

args = parser.parse_args()


def main():
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
    print(
        f"{len(list_of_wheels)} previously deployed wheel(s) found in workspace"
        f" path '{folder_path}'."
    )

    # filter "list of wheels" into a "list of to be deleted wheels"
    wheels_to_delete = get_wheels_to_delete(
        list_of_wheels=list_of_wheels,
        wheels_to_keep=args.nr_of_wheels_to_keep,
        nr_of_days_to_keep_wheel=args.nr_of_days_to_keep_wheel,
    )

    # Loop over all wheels (sorted on version) except the last x
    for wheel in wheels_to_delete:
        print(f"Deleting wheel '{wheel['path']}'.")
        delete_path(path=wheel["path"], host=args.host, http_header=http_header)
        sleep(1)  # Sleep for 1 second to avoid rate limiting

    print(f"Removed {len(wheels_to_delete)} wheel(s) from the workspace.")
    print("Done.")


if __name__ == "__main__":
    main()
