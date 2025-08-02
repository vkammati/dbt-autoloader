from edp_auto_loader.core.helpers.logger import get_logger
from edp_auto_loader.core.helpers.spark_init import spark

logger = get_logger(__name__)


def set_delta_table_properties(delta_table_properties: dict) -> None:
    """
    This function will take a dictionary of delta table properties and set each of them
    as spark session defaults. For a complete list check this:
    https://docs.databricks.com/en/delta/table-properties.html. This function should be
    called only once before loading any of the sources.
    """
    logger.info("Setting spark session delta table properties")
    logger.debug(f"Using this config '{delta_table_properties}'")

    # Set the properties for the spark session
    for k, v in delta_table_properties.items():
        logger.debug(f"Setting 'spark.databricks.delta.properties.defaults.{k}' to '{v}'")
        spark.sql(f"set spark.databricks.delta.properties.defaults.{k} = {v}")
