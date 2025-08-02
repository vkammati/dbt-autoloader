from typing import Dict

import yaml

from utils.check_virtual_environment import (
    check_virtual_environment as _check_virtual_environment,
)


def check_virtual_environment() -> None:
    _check_virtual_environment()


def get_yml(yml_path) -> Dict:
    """
    Ready yml
        Parameters:
            yml_path (str): path to yml file
        Returns:
            yml_dict (dict): dictionary with yml content
    """
    with open(yml_path, "r") as file:
        yml_dict = yaml.safe_load(file)

    if yml_dict is None:
        yml_dict = dict()

    return yml_dict
