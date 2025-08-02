# Add packages to the Python path to be able to import them.
import concurrent.futures
import os
import sys

sys.path.append(os.path.abspath(".."))

from edp_auto_loader.core.config import (  # noqa: E402
    get_auto_loader_config,
    get_load_config,
)
from edp_auto_loader.core.edp_auto_loader import (  # noqa: E402
    batch_load_table,
    get_final_table_list,
)
from edp_auto_loader.core.helpers.adls import set_storage_account_config  # noqa: E402
from edp_auto_loader.core.helpers.delta_table_properties import (  # noqa: E402
    set_delta_table_properties,
)
from edp_auto_loader.core.helpers.logger import get_logger, set_handler  # noqa: E402
from edp_auto_loader.core.helpers.utils import load_yml_files, merge_config  # noqa: E402

# Get logger and set handler
logger = get_logger("edp_auto_loader", "INFO")
set_handler(logger)


def _get_list_of_tables(auto_loader_config, table_filter, source_filter, full_load):
    """
    The function iterates through enabled source configurations and their tables,
    applying the specified filters. For each matching table, it retrieves the load
    configuration and adds it to the list of tables to load.
    Finally, it generates the final list of tables to be loaded.
    """
    logger.info("Start building list of tables to load")
    tables_to_load = []
    for source_config in [
        s
        for s in auto_loader_config["sources"]
        if (s.get("enabled") is None or s.get("enabled"))
        and (len(source_filter) == 0 or s.get("name") in source_filter)
    ]:
        for table_config in [
            t
            for t in source_config["tables"]
            if (t.get("enabled") is None or t.get("enabled"))
            and (len(table_filter) == 0 or t.get("source_table_folder") in table_filter)
        ]:

            logger.info(
                f'Adding {source_config.get("name")}'
                f' \\ {table_config["source_table_folder"]}'
            )
            load_config = get_load_config(
                table_config, source_config, auto_loader_config, full_load
            )
            # Add config to the list
            tables_to_load.append(load_config)

    final_table_list = get_final_table_list(tables_to_load)
    logger.info(f"No of tables in the config file: {len(tables_to_load)}")
    logger.info(f"No of tables to load: {len(final_table_list)}")
    return final_table_list


def _run_auto_loader(final_table_list):
    """
    This function uses a thread pool to concurrently load
    multiple tables based on the provided configurations.
    """

    logger.info("Entering the thread pool")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # executor = concurrent.futures.ThreadPoolExecutor()
        # Submit each table as a unique task using submit. This provides better control
        # than using 'map'
        logger.info("inside the thread pool")
        futures = [
            executor.submit(batch_load_table, load_config)
            for load_config in final_table_list
        ]
        # Iterate over all submitted tasks and get results as they are available to re-raise
        # any exceptions
        logger.info(f"Futures: {futures}")
        for future in concurrent.futures.as_completed(futures):
            try:
                logger.debug("Waiting for task to complete.")
                future.result()
                logger.debug(" task to completed.")
            except Exception as ex:
                logger.error(ex)
                raise

    logger.info(
        "Auto loader has successfully been started on all tables. Some might "
        "still be refreshing."
    )


# Entry point for this module when run from the workflow
def run():
    """
    This function is the entry point to this package when ran from a Databricks
    Workflow. It can only be started inside a wheel and will load the config
    yaml from its resources.
    """
    args = sys.argv[1:]
    logger.info(f"Running from Databricks Workflow with these arguments: {args}")

    source_filter = []
    if len(args) >= 1:
        source_filter = [a.strip() for a in args[0].split(",") if a.strip() != ""]

    table_filter = []
    if len(args) >= 2:
        table_filter = [a.strip() for a in args[1].split(",") if a.strip() != ""]
    full_load = None if len(args) < 3 or args[2] == "" else args[2].lower() == "true"
    log_level = "INFO" if len(args) < 4 or args[3] == "" else args[3].upper()

    if log_level:
        logger.setLevel(log_level)

    logger.info(
        f"Provided parameters: source_filter: '{source_filter}'"
        f", table_filter: '{table_filter}', full_load: '{full_load}'"
        f", log_level: '{log_level}'"
    )

    config_ymls = load_yml_files("edp_auto_loader", "config")
    config = merge_config(config_ymls)

    # Loading auto loader config yaml file
    auto_loader_config = get_auto_loader_config(config)

    # Setting the configured SPN properties on the storage account config setting
    # so it can be accessed.
    set_storage_account_config(
        auto_loader_config["general"]["az_storage_account"],
        auto_loader_config["general"]["secret_scope"],
        auto_loader_config["general"]["spn_tenant_id_key"],
        auto_loader_config["general"]["spn_client_id_key"],
        auto_loader_config["general"]["spn_client_secret_key"],
    )

    # Setting delta table default settings for the session if provided
    if auto_loader_config.get("delta_table_properties"):
        set_delta_table_properties(auto_loader_config["delta_table_properties"])

    # tables_to_load = _get_list_of_tables(
    #     auto_loader_config, table_filter, source_filter, full_load,
    # )

    # CHG : Added to handle the load type and source type
    tables_to_load = _get_list_of_tables(
        auto_loader_config, table_filter, source_filter, full_load
    )

    _run_auto_loader(tables_to_load)


# When this module is not run from a Databricks Workflow but
# interactively from the git repo this code below will be executed. Use it to
# test/write your yaml configurations. You can change the variables to load
# only specific sources or tables or to force full load.
if __name__ == "__main__":
    logger.info("Running from Databricks workspace UI")

    source_filter = []
    table_filter = []
    full_load = None
    log_level = "INFO"

    config_ymls = load_yml_files("edp_auto_loader", "config")
    config = merge_config(config_ymls)

    # Loading auto loader config yaml file
    auto_loader_config = get_auto_loader_config(config)

    # Setting the configured SPN properties on the storage account config setting
    # so it can be accessed.
    set_storage_account_config(
        auto_loader_config["general"]["az_storage_account"],
        auto_loader_config["general"]["secret_scope"],
        auto_loader_config["general"]["spn_tenant_id_key"],
        auto_loader_config["general"]["spn_client_id_key"],
        auto_loader_config["general"]["spn_client_secret_key"],
    )

    # Setting delta table default settings for the session if provided
    if auto_loader_config.get("delta_table_properties"):
        set_delta_table_properties(auto_loader_config["delta_table_properties"])

    tables_to_load = _get_list_of_tables(
        auto_loader_config, table_filter, source_filter, full_load
    )
    _run_auto_loader(tables_to_load)
