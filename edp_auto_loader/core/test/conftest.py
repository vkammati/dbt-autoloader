import os
import sys

import pytest
import yaml

abs_pkg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(abs_pkg_path)


@pytest.fixture(scope="session")
def test_get_missing_required_keys():
    return True


@pytest.fixture(scope="session")
def test_auto_loader_config():
    with open("edp_auto_loader/core/test/fixtures/auto_loader.yml", "r") as file:
        auto_loader = yaml.safe_load(file)
    return auto_loader
