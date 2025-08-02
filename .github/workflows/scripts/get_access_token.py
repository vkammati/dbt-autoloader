"""
This script is used to get (output is printed) the latest deployed version of a wheel
"""

import argparse

from helpers.azure.token import get_access_token

parser = argparse.ArgumentParser(description="Get access token")

parser.add_argument("--scope", help="Scope to request access token for", type=str)

args = parser.parse_args()


def get_token() -> str:
    # Get ad token token
    access_token = get_access_token()

    # return token
    return access_token


if __name__ == "__main__":
    access_token = get_token()
    print(access_token)
