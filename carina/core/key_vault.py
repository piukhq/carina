import logging

from typing import cast

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logger = logging.getLogger("key_vault")


class KeyVaultError(Exception):
    pass


class KeyVault:
    def __init__(self, vault_url: str, test_or_migration: bool = False) -> None:
        self.client: SecretClient | None = None
        self.vault_url = vault_url
        self.test_or_migration = test_or_migration
        if self.test_or_migration:
            logger.info("SecretClient will not be initialised as this is either a test or a migration.")

    def _get_client(self) -> SecretClient:
        return SecretClient(
            vault_url=self.vault_url,
            credential=DefaultAzureCredential(additionally_allowed_tenants=["a6e2367a-92ea-4e5a-b565-723830bcc095"]),
        )

    def get_secret(self, secret_name: str) -> str:
        if self.test_or_migration:
            return f"{secret_name}__testing-value"

        if not self.client:
            self.client = self._get_client()

        try:
            return cast(str, self.client.get_secret(secret_name).value)
        except (ServiceRequestError, ResourceNotFoundError, HttpResponseError) as ex:
            raise KeyVaultError(f"Could not retrieve secret {secret_name}") from ex
