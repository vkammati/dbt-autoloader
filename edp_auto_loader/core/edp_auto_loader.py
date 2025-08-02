from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType

from edp_auto_loader.core.helpers.logger import get_logger
from edp_auto_loader.core.helpers.spark_init import dbutils, spark

logger = get_logger(__name__)


def batch_load_table(load_config: dict) -> None:
    """
    This function will do the actual loading of the data. It expects a dictionary
    (load_config) with all the relevant properties and create a StreamReader to load
    the data and a StreamWriter to write the data in delta format to the storage account.
    """

    logger.info(
        "Started processing '"
        f'{load_config["general"]["dbx_unity_catalog"]}'
        f'.{load_config["general"]["dbx_raw_schema"]}'
        f'.{load_config["target_table_name"]}'
        "'"
    )
    logger.debug(f"Using load config '{load_config}'")

    # If a full_load of the source is requested we reset the current table
    if load_config["full_load"]:
        reset_status_and_truncate_data(
            load_config["source_checkpoint_path"],
            load_config["general"]["dbx_unity_catalog"],
            load_config["general"]["dbx_raw_schema"],
            load_config["target_table_name"],
        )

    # Create a StreamReader dataframe
    df = create_data_reader(load_config)

    df = df.withColumn("ingested_at", current_timestamp().cast("string"))

    # Add file name to the dataframe. This can be usefull later on in the transformation
    if load_config["target_add_file_name_column_to_output"]:
        df = df.withColumn(
            load_config["target_file_name_column_name"], col("_metadata.file_path")
        )

    # CHG: Handle casting of columns
    if load_config["target_cast_columns"]:
        # ToBeChanged
        logger.info("Casting columns")
        df = cast_columns(load_config, df)

    # By default, managed tables are created in UC. However, a table can be configured to
    # be created as external table as well by setting 'use_external_table=true'.
    if load_config["target_use_external_table"]:
        logger.debug("Creating EXTERNAL table instead of MANAGED table.")
        # Create a UC external table (if it does not exist yet) based on the schema read
        # from landing. The table is created before writing the actual data to prevent
        # 'ProtocolChangedException' that can be caused by having multiple writer writing
        # to the same empty directory.
        create_external_table(
            load_config["target_path"],
            load_config["general"]["dbx_unity_catalog"],
            load_config["general"]["dbx_raw_schema"],
            load_config["target_table_name"],
            df.schema,
            load_config["target_partition_by"],
        )

    # CHG: Vacuum the table to remove old files
    if load_config["target_vacuum"]:
        logger.info("Vacuuming the table")
        if load_config["target_vacuum_retention_hours"]:
            if load_config["target_vacuum_retention_hours"] < 168:
                if not load_config["target_vacuum_retention_check"]:
                    spark.conf.set(
                        "spark.databricks.delta.retentionDurationCheck.enabled", "false"
                    )
                    logger.info(
                        f"Vacuuming the table {load_config['target_table_name']}"
                        f" with retention period of {load_config['target_vacuum_retention_hours']}"
                        " hours"
                    )
                    vacuum_statement = (
                        f"VACUUM `{load_config['general']['dbx_unity_catalog']}`"
                    )
                    vacuum_statement += f".`{load_config['general']['dbx_raw_schema']}`"
                    vacuum_statement += f".`{load_config['target_table_name']}`"
                    vacuum_statement += (
                        f" RETAIN {load_config['target_vacuum_retention_hours']} HOURS"
                    )
                    spark.sql(vacuum_statement)
                    spark.conf.set(
                        "spark.databricks.delta.retentionDurationCheck.enabled", "true"
                    )
                else:
                    logger.info("Vacuum failed as retention period is less than 7 days")
            else:
                logger.info(
                    f"Vacuuming the table {load_config['target_table_name']} "
                    f"with retention period of {load_config['target_vacuum_retention_hours']}"
                    " hours"
                )
                vacuum_statement = (
                    f"VACUUM `{load_config['general']['dbx_unity_catalog']}`"
                )
                vacuum_statement += f".`{load_config['general']['dbx_raw_schema']}`"
                vacuum_statement += f".`{load_config['target_table_name']}`"
                vacuum_statement += (
                    f" RETAIN {load_config['target_vacuum_retention_hours']} HOURS"
                )
                spark.sql(vacuum_statement)
        else:
            logger.info(
                f"Vacuuming the table {load_config['target_table_name']}"
                "with default retention period"
            )
            vacuum_statement = f"VACUUM `{load_config['general']['dbx_unity_catalog']}`"
            vacuum_statement += f".`{load_config['general']['dbx_raw_schema']}`"
            vacuum_statement += f".`{load_config['target_table_name']}`"
            spark.sql(vacuum_statement)

    # Finally, create and execute a StreamWriter dataframe to write the data to the
    # deltalake container
    create_data_writer(load_config, df)

    logger.info(
        "Auto Loader job started to load '"
        f'{load_config["general"]["dbx_unity_catalog"]}'
        f'.{load_config["general"]["dbx_raw_schema"]}'
        f'.{load_config["target_table_name"]}'
        "'"
    )


def reset_status_and_truncate_data(
    full_checkpoint_path: str, catalog: str, schema: str, table: str
) -> None:
    """
    This function is used to "reset" a table loaded through Auto Loader. This can be used
    to clean up (truncate) a table and its status before reloading it. Note that this
    does not remove the table.
    """

    logger.info(
        "Requested full load. Removing checkpoints files and truncating target table"
    )

    # To reset the table, two things need to happen:
    # - The checkpoint file for this source need to be removed. This way, Auto loader
    #   will try to load all files in the next run
    # - The table in UC needs to be truncated to prevent duplicate data

    # first clear the checkpoints folder
    logger.debug(f"Deleting checkpoint path '{full_checkpoint_path}'")
    dbutils.fs.rm(full_checkpoint_path, recurse=True)

    # Then truncate the table. First we check to make sure  (if it exists)
    df_tables = spark.sql(f"show tables in `{catalog}`.`{schema}`")
    df_tables = df_tables.filter(df_tables.tableName == table.lower())
    if df_tables.count() > 0:
        logger.debug(f"Truncating table '`{catalog}`.`{schema}`.`{table}`'")
        spark.sql(f"truncate table `{catalog}`.`{schema}`.`{table}`")


def create_data_reader(load_config: dict) -> DataFrame:
    """
    This function translates a load config into a Auto Loader read stream.
    """

    # Create StreamReader dataframe
    df = spark.readStream.format("cloudFiles")

    # Fetch the read option so they can be appended to. Read_options is a
    # required config option.
    read_options = load_config["read_options"]

    # If a schema is supplied, use it. Otherwise infer the schema.
    if load_config["source_schema"]:
        logger.debug(f'Applying schema: {load_config["source_schema"]}')
        schema = StructType.fromJson(load_config["source_schema"])
        df = df.schema(schema)
    else:
        logger.debug("Infering schema")
        if "cloudFiles.inferColumnTypes" not in read_options.keys():
            read_options["cloudFiles.inferColumnTypes"] = True

    # Check provided 'read_options' and add the required onces that were not added
    # manually
    if "cloudFiles.schemaLocation" not in read_options.keys():
        read_options["cloudFiles.schemaLocation"] = load_config["source_schema_path"]

    # Apply all read_options to the stream
    for k, v in read_options.items():
        logger.debug(f"Read setings {k} to {v}")
        df = df.option(k, v)

    # Finalizing StreamReader and start loading the source file(s)
    df = df.load(load_config["source_path"])

    return df


def create_data_writer(load_config: dict, reader_df: DataFrame) -> None:
    """
    This function translates a load config and DataReader DataFrame into a Auto Loader
    rearrite stream and finally executes it.write_options
    """

    # Fetch the write option so they can be appended to
    write_options = load_config.get("write_options")
    if write_options is None:
        write_options = dict()

    # Create streamWriter dataframe
    # write the data using a 'availableNow' trigger
    df = reader_df.writeStream.format("delta")
    if not load_config.get("streaming"):
        logger.debug(
            "Setting availableNow property to make this a batch process; not streaming."
        )
        df = df.trigger(availableNow=True)

    # Check provided 'write_options' and add the required onces that were
    # not added manually
    if "checkpointLocation" not in write_options.keys():
        write_options["checkpointLocation"] = load_config["source_checkpoint_path"]

    # By default, set 'mergeSchema' to true as this might cause problems
    # during the initial load otherwise
    if "mergeSchema" not in write_options.keys():
        write_options["mergeSchema"] = "true"

    # Apply all write_options to the stream
    for k, v in write_options.items():
        logger.debug(f"Write setings {k} to {v}")
        df = df.option(k, v)

    if load_config.get("target_partition_by"):
        logger.debug(f'setting partitionBy to {load_config.get("target_partition_by")}')
        df = df.partitionBy(load_config.get("target_partition_by"))

    # Start writing
    if load_config["target_use_external_table"]:
        query = df.start(load_config["target_path"])
        query.awaitTermination()
        logger.info(
            "External table created and data loaded: " + load_config["target_table_name"]
        )
    else:
        catalog = load_config["general"]["dbx_unity_catalog"]
        schema = load_config["general"]["dbx_raw_schema"]
        table = load_config["target_table_name"]
        df.toTable(f"`{catalog}`.`{schema}`.`{table}`")

    if load_config["target_compaction"]:
        logger.info(f"Compacting the table {load_config['target_table_name']}")
        optimize_statement = f"OPTIMIZE `{load_config['general']['dbx_unity_catalog']}`"
        optimize_statement += f".`{load_config['general']['dbx_raw_schema']}`"
        optimize_statement += f".`{load_config['target_table_name']}`"
        if load_config["target_partition_by"]:
            optimize_statement += f" ZORDER BY ({load_config['target_partition_by']})"
        spark.sql(optimize_statement)


def create_external_table(
    location: str,
    catalog: str,
    schema: str,
    table: str,
    schema_structure: StructType,
    partition_by_columns: list,
) -> None:
    """
    This function creates an external table refering to the delta lake location. It can
    be used before and after creating the delta lake files.
    """

    # first extract the column specification out of the provided schema structure
    column_specification = [
        f"`{c.name}` {c.dataType.simpleString()}" for c in schema_structure
    ]

    # Build the dynamic sql statement that is executed to create the table
    sql_create_statement = f"create table if not exists `{catalog}`.`{schema}`.`{table}`"
    sql_create_statement += f" ({', '.join(column_specification)})"
    sql_create_statement += " using delta"
    sql_create_statement += f" location '{location}'"
    if partition_by_columns:
        sql_create_statement += (
            " partitioned by (" + ", ".join(partition_by_columns) + ")"
        )

    # Execute the sql statement
    logger.debug(f"Executing sql statement '{sql_create_statement}'")
    spark.sql(sql_create_statement)


def get_final_table_list(tables_to_load: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Returns a list of tables where the corresponding source path exists and contains files.
    Returns:
    - List of dictionaries representing tables where the source path exists and is not empty.
    """
    final_table_list = []

    for table in tables_to_load:

        path_to_check = table["source_path"][:-2]

        try:
            # List the contents of the directory
            files = dbutils.fs.ls(path_to_check)

            # Check if the directory is not empty
            if files:
                # print(f"{table['target_table_name']} folder exists and contains files")
                final_table_list.append(table)
            else:
                logger.info(
                    f"Skipping {table['target_table_name']} folder as it is empty"
                )
                # Optionally append to the list if the folder is empty
                # final_table_list.append(table)
        except Exception as e:
            # Handle the case where the directory does not exist or other issues
            logger.info(f">>> WARNING: Path is not valid: {path_to_check} <<<")
            logger.info(e)

    # Return the final list
    return final_table_list


def cast_columns(load_config: dict, df: DataFrame) -> DataFrame:
    """
    This function is used to cast columns in a dataframe to a specific type.
    """
    for column in load_config["cast_columns"]["columns"]:
        # ToBeChanged
        logger.info(
            f"Casting column '{column['column_name']}' to type '{column['data_type']}'"
        )
        df = df.withColumn(
            column["column_name"], col(column["column_name"]).cast(column["data_type"])
        )

    return df
