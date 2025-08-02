from edp_auto_loader.core.helpers.logger import get_logger
from edp_auto_loader.core.helpers.spark_init import dbutils, spark

logger = get_logger(__name__)


def set_storage_account_config(
    storage_account: str,
    secret_scope: str,
    spn_tenant_id_key: str,
    spn_client_id_key: str,
    spn_client_secret_key: str,
) -> None:
    """
    This function will fetch the SPN information from key vault using the provided key
    names and scope and use it to configure Spark to the use this SPN when connection
    to the given ADLS Gen2 storage account.
    """
    logger.info(f"Setting spark config for storage account '{storage_account}'")

    spn_tenant_id = dbutils.secrets.get(scope=secret_scope, key=spn_tenant_id_key)
    spn_client_id = dbutils.secrets.get(scope=secret_scope, key=spn_client_id_key)
    spn_client_secret = dbutils.secrets.get(scope=secret_scope, key=spn_client_secret_key)

    spark.conf.set(
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
        spn_client_id,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
        spn_client_secret,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{spn_tenant_id}/oauth2/token",
    )
