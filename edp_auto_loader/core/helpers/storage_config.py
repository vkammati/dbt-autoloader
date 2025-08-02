from edp_auto_loader.core.helpers.logger import get_logger
from edp_auto_loader.core.helpers.utils import strip_slashes

logger = get_logger(__name__)


def get_source_container() -> str:
    """
    Return the the name of the container Auto Loader will read the source
    files from
    """

    return "landing"


def get_source_subfolder(table_config: dict, environment: str) -> str:
    """
    Return the the name of the subfolder Auto Loader will read the source
    files from
    """

    sub_folder_source = table_config["source_table_folder_prefixes"][environment]

    return sub_folder_source


def get_target_container() -> str:
    """
    Return the the name of the container Auto Loader will write the delta
    files to in case external tables are use instead of managed tables
    """

    return "deltalake"


def get_target_subfolder() -> str:
    """
    Return the the name of the container Auto Loader will write the delta
    files to in case external tables are use instead of managed tables
    """

    return "ds_dbt"


def get_base_source_path(general_config: dict, table_config: dict) -> str:
    """
    Return the base path of any source location based on the general
    and table config dictionaries
    """

    container = get_source_container()
    storage_account = general_config["az_storage_account"]
    confidentiality = table_config["confidentiality"]
    source_folder_name = table_config["root_source"]
    environment = general_config["environment"]
    sub_folder = get_source_subfolder(table_config, environment)

    path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    path += f"/{confidentiality}"
    path += f"/{source_folder_name}_{environment}"
    if sub_folder:
        path += f"/{sub_folder}"

    return path


def get_base_target_path(
    general_config: dict, target_container_subfolder: str, table_config: dict
) -> str:
    """
    Return the base path of any target location based on the general
    and table config dictionaries. This is only used in case external
    tables are use instead of managed tables
    """

    container = get_target_container()
    storage_account = general_config["az_storage_account"]
    environment = general_config["environment"]
    sub_folder = get_target_subfolder()

    # In older edp setups, the raw layer can have it own container and therefore no
    # subfolder. If so, the fixed folder name should be left empty. This bit will
    # make sure this function will still yield a valid path.
    if target_container_subfolder and target_container_subfolder != "":
        target_container_subfolder = "/" + target_container_subfolder

    path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    # path += f"{target_container_subfolder}/{project_name}_{environment}"
    path += f"{target_container_subfolder}/{sub_folder}_{environment}"

    return path


def get_source_table_path(general_config: dict, table_config: dict) -> str:
    """
    Return full path to source based on the general and table config
    dictionaries
    """

    path = get_base_source_path(general_config, table_config)
    path += f'/{strip_slashes(table_config["source_table_folder"])}/*'

    return path


# CHG: Added to handle the load type and source type
def get_metadata_source_path(general_config: dict, table_config: dict) -> str:
    """
    Return full path to metadata location based on the general and table
    config dictionaries. At this location both checkpoints and schema data
    will be stored in separate folders. Change this function to store the
    metadata in a different location.
    """

    path = get_base_source_path(general_config, table_config)
    path += f'/auto_loader_metadata/{strip_slashes(table_config["source_table_folder"])}'

    return path


# CHG: Added to handle the load type and source type
def get_checkpoint_path(general_config: dict, table_config: dict) -> str:
    """
    Return full path to checkpoint location based on the general
    and table config dictionaries
    """

    path = get_metadata_source_path(general_config, table_config)
    path += "/checkpoint"

    return path


# CHG: Added to handle the load type and source type
def get_schema_path(general_config: dict, table_config: dict) -> str:
    """
    Return full path to schema location based on the general
    and table config dictionaries
    """

    path = get_metadata_source_path(general_config, table_config)
    path += "/schema"

    return path
