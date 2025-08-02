import os
from typing import Optional

from edp_auto_loader.core.helpers.logger import get_logger
from edp_auto_loader.core.helpers.storage_config import (
    get_base_target_path,
    get_checkpoint_path,
    get_schema_path,
    get_source_table_path,
)
from edp_auto_loader.core.helpers.utils import get_missing_required_keys, strip_slashes

logger = get_logger(__name__)


_fixed_target_container_subfolder = "raw"
_fixed_target_table_prefix = ""
_fixed_file_name_column_name = "source_file_name"


def get_general_config() -> dict:
    """
    Get all required environment variable and raise an error if one of
    them is not set or is empty. Return the env vars in one dictionary.
    """

    # Check if all required environment variables are set
    general = {}
    required_environment_variables = [
        "environment",
        "az_storage_account",
        "dbx_unity_catalog",
        "dbx_raw_schema",
        "project_name",
    ]

    for env_var in required_environment_variables:
        general[env_var] = os.getenv(env_var.upper())
        if not general.get(env_var) or general.get(env_var) == "":
            raise Exception(f"Missing envrionment variable '{env_var.upper()}'.")

    # These settings refer to the secret scope and secrets created using terraform.
    # When changed, they should also be updated in the terraform code.
    general["secret_scope"] = (
        f"{general['project_name']}_{general['environment']}_edp_auto_loader"
    )
    general["spn_tenant_id_key"] = "AZ-RUN-SPN-TENANT-ID"
    general["spn_client_id_key"] = "AZ-RUN-SPN-CLIENT-ID"
    general["spn_client_secret_key"] = "AZ-RUN-SPN-CLIENT-SECRET"

    return general


def check_raise_required_keys(
    dictionary_name: str, dictionary_to_check: dict, required_keys: list[str]
):
    """
    Helper function to check if a dictionary contains the required list
    of keys. If not, an execption is raised.
    """

    missing_required_fields = get_missing_required_keys(
        dictionary_to_check,
        required_keys,
    )
    if missing_required_fields:
        raise Exception(
            f"Missing or empty required key(s) in '{dictionary_name}' configuration: "
            f"{missing_required_fields}"
        )


def get_auto_loader_config(config: dict) -> dict:
    """
    Load the yaml file provided at the path provided, validate its content and return
    the 'auto_loader' key from it.
    """

    try:
        # validate the yaml schema and content
        auto_loader_config = config.get("auto_loader")
        if not auto_loader_config:
            raise Exception(
                "Missing top-level node 'auto_loader'. The yaml file should contain node"
                " named 'auto_loader' which must contain the Auto Loader configuration."
            )

        # Checking top-level required fields
        check_raise_required_keys(
            "auto_loader",
            auto_loader_config,
            [
                "sources",
            ],
        )

        # Validating all sources
        for source_config in auto_loader_config["sources"]:
            # Checking source-level required fields
            source_name = source_config.get("name")
            check_raise_required_keys(source_name, source_config, ["tables"])

            # Validating all tables
            for table_config in source_config["tables"]:
                # Most of the table config properties can also be set on the source level.
                # Therefore we create a combined config first and check and validate it.
                table_config = merge_source_and_table_config(table_config, source_config)
                source_table_folder = table_config.get("source_table_folder")

                # Checking table-level required fields
                check_raise_required_keys(
                    source_table_folder,
                    table_config,
                    ["source_table_folder", "confidentiality", "read_options"],
                )

                # checking required read options
                check_raise_required_keys(
                    f"read_options of {source_table_folder}",
                    table_config["read_options"],
                    ["cloudFiles.format"],
                )

                # Checking required values
                if table_config["confidentiality"] not in ["confidential", "internal"]:
                    raise Exception(
                        f'Unknown confidentiality \'{table_config["confidentiality"]}\'.'
                        ' Value must be "confidential" or "internal".'
                    )

                if table_config["read_options"]["cloudFiles.format"] not in [
                    "json",
                    "csv",
                    "parquet",
                    "avro",
                    "orc",
                    "text",
                    "binaryfile",
                ]:
                    raise Exception(
                        f'Unknown format \'{table_config["read_options"]["format"]}\'.'
                        ' Value must be "json", "csv", "parquet", "avro", "orc", '
                        '"text" or "binaryfile".'
                    )

        # Check if all required environment variables are set and add it as general config
        general_config = get_general_config()

        # Checking required values
        if general_config["environment"] not in ["dev", "tst", "pre", "uat", "prd"]:
            raise Exception(
                f'Unknwon environment \'{general_config["environment"]}\'.'
                ' Value must be "dev", "tst", "pre", "uat" or "prd".'
            )

        auto_loader_config["general"] = general_config

    except Exception as ex:
        logger.error(ex)
        raise

    # Return the validated config
    logger.info("Auto Loader config succesfully loaded and validated.")
    return auto_loader_config


def merge_source_and_table_config(table_config: dict, source_config: dict) -> dict:
    """
    Most properties provided at the table level can also be provided at the source level.
    This makes it easier to configure and maintain an entire source. This function takes
    the table config and adds the source level properties to it if not present in the
    tabel config. For first level child dictionaries, the same principal applies where
    items of the source config will be added to the table sub key.
    """

    # First create a copy of he dictionary to prevent overwriting it
    table_config_copy = table_config.copy()

    # loop over all items
    for key, val in source_config.items():
        if (
            not isinstance(val, dict)
            and key not in table_config_copy.keys()
            and key not in ["name", "tables"]
        ):
            # if the key is not a dict, and it is not in the table config and is not in
            # the list of keys that have no value at the table level, we add the key to
            # the table config.
            table_config_copy[key] = val
        elif isinstance(val, dict):
            # If the key is a dictionary, we loop over all of its items.
            for sub_key, sub_val in val.items():
                # We first make sure this key is also available in the table config
                if key not in table_config_copy.keys():
                    table_config_copy[key] = dict()
                # Then we can set its value
                table_config_copy[key][sub_key] = sub_val

    return table_config_copy


# CHG: Added to handle the load type and source type
def get_load_config(
    table_config: dict,
    source_config: dict,
    auto_loader_config: dict,
    full_load: Optional[bool] = None,
) -> dict:
    """
    Transform provided table config into a dictionary that contains all properties needed
    to start auto loader. The provided 'source' and 'auto_loader' config are use to set
    default or more generic settings. The full_load parameter can be used to force a
    'full load', overwriting what is specified in the configs.
    """

    # The load config will hold all the (validated) parameters needed to perform a load
    load_config = {}

    # Load general config settings
    load_config["general"] = auto_loader_config["general"]

    # Most properties provided at the table level can also be provided at the source
    # level. This makes it easier to configure and maintain an entire source. To prevent
    # having to check for both property, we simply update the table config with all
    # (relevant) properties in the source_config unless the key already exists.
    table_config = merge_source_and_table_config(table_config, source_config)

    # Determine source elements and update the load_config with it
    source_table_folder = strip_slashes(table_config["source_table_folder"])

    # CHG: Added to handle the load type and source type
    load_config["source_path"] = get_source_table_path(
        load_config["general"], table_config
    )
    load_config["source_checkpoint_path"] = get_checkpoint_path(
        load_config["general"], table_config
    )
    load_config["source_schema_path"] = get_schema_path(
        load_config["general"], table_config
    )
    load_config["source_schema"] = table_config.get("schema")

    # Determine target elements and update the load_config with it
    base_target_path = get_base_target_path(
        load_config["general"], _fixed_target_container_subfolder, table_config
    )

    # CHG: Added to change target table name based on source type
    target_table_name = _fixed_target_table_prefix
    target_table_name += (
        table_config["alt_target_table_name"]
        if table_config.get("alt_target_table_name") is not None
        else source_table_folder.replace("/", "__")
    )
    target_add_file_name = (
        table_config["add_file_name_column_to_output"]
        if table_config.get("add_file_name_column_to_output") is not None
        else False
    )

    load_config["target_use_external_table"] = False
    if table_config.get("use_external_table") is True:
        load_config["target_use_external_table"] = True

    load_config["streaming"] = False
    if table_config.get("streaming") is True:
        load_config["streaming"] = True

    # CHG: Casting columns check
    load_config["target_cast_columns"] = False
    if table_config.get("cast_columns") is not None:
        load_config["target_cast_columns"] = True
        load_config["cast_columns"] = table_config.get("cast_columns")

    load_config["target_vacuum"] = False
    load_config["target_vacuum_retention_hours"] = None
    load_config["target_vacuum_retention_check"] = True
    if table_config.get("vacuum") is not None:
        if table_config.get("vacuum").get("enabled") is not None:
            load_config["target_vacuum"] = table_config.get("vacuum").get("enabled")
        if (
            table_config.get("vacuum").get("retention_hours") is not None
            and load_config["target_vacuum"]
        ):
            load_config["target_vacuum_retention_hours"] = table_config.get("vacuum").get(
                "retention_hours"
            )
        if (
            table_config.get("vacuum").get("retention_check") is not None
            and load_config["target_vacuum"]
        ):
            load_config["target_vacuum_retention_check"] = table_config.get("vacuum").get(
                "retention_check"
            )

    load_config["target_compaction"] = False
    if table_config.get("compaction") is not None:
        load_config["target_compaction"] = table_config.get("compaction")

    load_config["target_table_name"] = target_table_name
    load_config["target_path"] = f"{base_target_path}/{target_table_name}"
    load_config["target_add_file_name_column_to_output"] = target_add_file_name
    load_config["target_file_name_column_name"] = _fixed_file_name_column_name
    load_config["target_partition_by"] = table_config.get("target_partition_by")

    # Add the read_options and write_options to the load config
    load_config["read_options"] = table_config.get("read_options")
    load_config["write_options"] = table_config.get("write_options")

    # The 'full load' property can be set using the config or provided at runtime
    load_config["full_load"] = (
        table_config.get("full_load") if full_load is None else full_load
    )

    return load_config
