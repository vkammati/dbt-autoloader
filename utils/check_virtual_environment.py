import os
import sys


def check_virtual_environment() -> None:
    """
    Check if python virtual environment is active.
    Stop execution if it isn't.
    """
    if (
        os.getenv("GITHUB_ACTIONS") == "true"
        or hasattr(sys, "real_prefix")
        or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix)
    ):
        print("Virtual environment is active")
    else:
        raise EnvironmentError(
            "Please, activate a virtual environment before execute this script"
        )
