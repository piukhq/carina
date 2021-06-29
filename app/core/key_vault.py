import logging

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logger = logging.getLogger("key_vault")


class KeyVaultError(Exception):
    pass


class KeyVault:
    def __init__(self, vault_url: str, test_or_migration: bool = False) -> None:
        if test_or_migration:
            self.client = None
            logger.info("Key Vault not initialised as this is either a test or a migration.")
        else:
            self.client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())

    def get_secret(self, secret_name: str) -> str:
        if not self.client:
            return "testing-token"

        try:
            return self.client.get_secret(secret_name).value
        except (ServiceRequestError, ResourceNotFoundError, HttpResponseError) as ex:
            raise KeyVaultError(f"Could not retrieve secret {secret_name}") from ex
