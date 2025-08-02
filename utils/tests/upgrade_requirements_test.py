from utils.upgrade_requirements import _get_requirement_packages


def test__get_requirement_packages(test_requirements_path):
    requirement_packages = _get_requirement_packages(test_requirements_path)
    is_dict = isinstance(requirement_packages, dict)
    is_not_empty = len(requirement_packages) > 0
    is_all_k_v_not_none = all(
        k is not None and v is not None for k, v in requirement_packages.items()
    )
    assert is_dict and is_not_empty and is_all_k_v_not_none
