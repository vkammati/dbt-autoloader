from edp_auto_loader.core.helpers.utils import (
    get_missing_required_keys,
    load_yml_files,
    merge_config,
)


def test_load_yml_files():
    config_files = load_yml_files("edp_auto_loader", "core/test/fixtures/auto_loader")
    is_dict = isinstance(config_files, dict)
    is_not_empty = len(config_files) > 0
    is_all_k_v_not_none = all(
        k is not None and v is not None for k, v in config_files.items()
    )
    assert is_dict and is_not_empty and is_all_k_v_not_none


def test_merge_config(test_auto_loader_config):
    config_files = load_yml_files("edp_auto_loader", "core/test/fixtures/auto_loader")
    config = merge_config(config_files)

    is_dict = isinstance(config, dict)
    is_not_empty = len(config) > 0
    is_all_k_v_not_none = all(k is not None and v is not None for k, v in config.items())

    assert is_dict and is_not_empty and is_all_k_v_not_none

    # Make sure sources are ordered by their name before comparions
    config["auto_loader"]["sources"] = sorted(
        config["auto_loader"]["sources"], key=lambda x: x["name"]
    )
    test_auto_loader_config["auto_loader"]["sources"] = sorted(
        test_auto_loader_config["auto_loader"]["sources"], key=lambda x: x["name"]
    )

    assert config == test_auto_loader_config


def test_get_missing_required_keys():
    test_required_keys_1 = ["key1", "key2"]
    test_required_keys_2 = ["key1", "key2", "key3"]
    test_required_keys_3 = ["key1", "key2", "key3", "key4"]

    def test_fn(check_keys, without_none):
        test_dic = {"key1": "value1", "key2": "value2", "key3": None}
        return get_missing_required_keys(test_dic, check_keys, without_none)

    res_with_empty_values = test_fn(test_required_keys_1, False)
    res_without_none = test_fn(test_required_keys_1, True)

    res_with_empty_values_2 = test_fn(test_required_keys_2, False)
    res_without_none_2 = test_fn(test_required_keys_2, True)

    res_with_empty_values_3 = test_fn(test_required_keys_3, False)
    res_without_none_3 = test_fn(test_required_keys_3, True)

    assert res_with_empty_values == []
    assert res_without_none == []

    assert res_with_empty_values_2 == []
    assert res_without_none_2 == ["key3"]

    assert res_with_empty_values_3 == ["key4"]
    assert len([i for i in res_without_none_3 if i in ["key3", "key4"]]) == 2
