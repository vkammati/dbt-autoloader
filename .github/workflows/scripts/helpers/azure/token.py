import base64
import os

from azure.identity import CertificateCredential, DefaultAzureCredential

# default scope is set to Databricks
_default_scope = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"


def get_access_token(scope: str = None) -> str:
    # This flow uses the azure-identity library to implement various forms
    # of authentication
    credential = None

    # If scope is not supplied, use the default scope
    if not scope:
        scope = _default_scope

    # The DefaultCredential flow expects the AZURE_CLIENT_CERTIFICATE_PATH to be set
    # when using the certficate. This workaround makes it possible to also set the content
    # of the certificate as base64 string in 'AZURE_CLIENT_CERTIFICATE' which will then
    # be used in conjunction with the other default environment variables. This is easier
    # to use from github workflow and prevents having to commit the certificate to disk.
    # All "standard" envrionment variables can be found here:
    # https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/identity/azure-identity#environment-variables # noqa: E501
    if os.getenv("AZURE_CLIENT_CERTIFICATE"):
        # Encode string into bytes
        certificate_data = base64.b64decode(
            os.getenv("AZURE_CLIENT_CERTIFICATE").encode("ascii")
        )
        # Create the credentials using the 'other' environment variables. the password
        # is stripped and encoded. This makes it possible to work with certificates that
        # are not password protected by simply not adding the password GitHub secret or
        # leaving it an empty string.
        credential = CertificateCredential(
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            certificate_data=certificate_data,
            password=os.getenv("AZURE_CLIENT_CERTIFICATE_PASSWORD")
            .strip()
            .encode("utf-8"),
        )
    else:
        # Use the defaul credential flow as back-up. This will be used with the
        # client_secret or the azure cli flow for instance. Make sure to set the
        # appropriate environment variables or add tasks to the workflow to login
        credential = DefaultAzureCredential(logging_enable=True)

    # Now fetch a token adn return it.
    try:
        ad_token = credential.get_token(scope)
        return ad_token.token
    except Exception:
        raise
