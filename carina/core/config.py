import logging
import os
import sys

from logging.config import dictConfig
from typing import Any, Literal

import sentry_sdk

from pydantic import AnyHttpUrl, BaseSettings, Field, HttpUrl, PostgresDsn, validator
from redis import Redis
from retry_tasks_lib.settings import load_settings
from sentry_sdk.integrations.redis import RedisIntegration

from carina.core.key_vault import KeyVault
from carina.version import __version__

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LogLevels = Literal["CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "NOTSET"]


def _get_command() -> str:
    cmd = sys.argv[0]
    if cmd == "poetry":
        cmd = sys.argv[2] if len(sys.argv) > 2 else "None"
    return cmd


_COMMAND = _get_command()


class Settings(BaseSettings):
    API_PREFIX: str = "/rewards"
    SQL_DEBUG: bool = False
    TESTING: bool = False

    @validator("TESTING")
    @classmethod
    def is_test(cls, v: bool) -> bool:
        return "test" in _COMMAND or v

    MIGRATING: bool = False

    @validator("MIGRATING")
    @classmethod
    def is_migration(cls, v: bool) -> bool:
        return "alembic" in _COMMAND or v

    PROJECT_NAME: str = "carina"
    ROOT_LOG_LEVEL: LogLevels | None = None
    QUERY_LOG_LEVEL: LogLevels | None = None
    LOG_FORMATTER: Literal["json", "brief", "console"] = "json"
    KEY_VAULT_URI: str = "https://bink-uksouth-dev-com.vault.azure.net/"
    KEY_VAULT: KeyVault = None  # type: ignore [assignment]

    @validator("KEY_VAULT", pre=True, always=True)
    @classmethod
    def load_key_vault(cls, _: None, values: dict[str, Any]) -> KeyVault:
        return KeyVault(values["KEY_VAULT_URI"], values["TESTING"] or values["MIGRATING"])

    CARINA_API_AUTH_TOKEN: str = None  # type: ignore [assignment]

    @validator("CARINA_API_AUTH_TOKEN", pre=True, always=True)
    @classmethod
    def fetch_carina_api_auth_token(cls, v: str | None, values: dict[str, Any]) -> str:
        if v is not None:
            return v

        return values["KEY_VAULT"].get_secret("bpl-carina-api-auth-token")

    POLARIS_API_AUTH_TOKEN: str = None  # type: ignore [assignment]

    @validator("POLARIS_API_AUTH_TOKEN", pre=True, always=True)
    @classmethod
    def fetch_polaris_api_auth_token(cls, v: str | None, values: dict[str, Any]) -> str:
        if v is not None:
            return v

        return values["KEY_VAULT"].get_secret("bpl-polaris-api-auth-token")

    USE_NULL_POOL: bool = False

    @validator("USE_NULL_POOL")
    @classmethod
    def set_null_pool(cls, v: bool, values: dict[str, Any]) -> bool:
        return values["TESTING"] or v

    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = "carina"
    SQLALCHEMY_DATABASE_URI: str = None  # type: ignore [assignment]

    @validator("SQLALCHEMY_DATABASE_URI", pre=True, always=True)
    @classmethod
    def assemble_db_connection(cls, v: str | None, values: dict[str, Any]) -> str:

        if v is not None:
            db_uri = v.format(values["POSTGRES_DB"])

        else:
            db_uri = PostgresDsn.build(
                scheme="postgresql",
                user=values.get("POSTGRES_USER"),
                password=values.get("POSTGRES_PASSWORD"),
                host=values.get("POSTGRES_HOST"),
                port=values.get("POSTGRES_PORT"),
                path="/" + values.get("POSTGRES_DB", ""),
            )

        if values["TESTING"]:
            db_uri += "_test"

        return db_uri

    SQLALCHEMY_DATABASE_URI_ASYNC: str = None  # type: ignore [assignment]

    @validator("SQLALCHEMY_DATABASE_URI_ASYNC", pre=True, always=True)
    @classmethod
    def adapt_db_connection_to_async(cls, v: str | None, values: dict[str, Any]) -> str:
        if v is not None:
            db_uri = v.format(values["POSTGRES_DB"])
        else:
            db_uri = (
                values["SQLALCHEMY_DATABASE_URI"]
                .replace("postgresql://", "postgresql+asyncpg://")
                .replace("sslmode=", "ssl=")
            )

        return db_uri

    DB_CONNECTION_RETRY_TIMES: int = 3
    SENTRY_DSN: HttpUrl | None = None

    @validator("SENTRY_DSN", pre=True, always=True)
    @classmethod
    def sentry_dsn_can_be_blank(cls, v: str | None) -> str | None:
        if v is not None and len(v) == 0:
            return None
        return v

    SENTRY_ENV: str | None = None
    SENTRY_TRACES_SAMPLE_RATE: float = Field(0.0, ge=0.0, le=1.0)

    POLARIS_HOST: str = "http://polaris-api"
    POLARIS_BASE_URL: str = None  # type: ignore [assignment]

    @validator("POLARIS_BASE_URL", pre=True, always=True)
    @classmethod
    def polaris_base_url(cls, v: str | None, values: dict[str, Any]) -> str:
        if v is not None:
            return v

        return f"{values['POLARIS_HOST']}/loyalty"

    REDIS_URL: str

    @validator("REDIS_URL")
    @classmethod
    def assemble_redis_url(cls, v: str, values: dict[str, Any]) -> str:

        if values["TESTING"]:
            base_url, db_n = v.rsplit("/", 1)
            return f"{base_url}/{int(db_n) + 1}"

        return v

    BLOB_STORAGE_DSN: str = ""
    BLOB_IMPORT_CONTAINER = "carina-imports"
    BLOB_ARCHIVE_CONTAINER = "carina-archive"
    BLOB_ERROR_CONTAINER = "carina-errors"
    BLOB_IMPORT_SCHEDULE = "*/5 * * * *"
    BLOB_CLIENT_LEASE_SECONDS = 60
    BLOB_IMPORT_LOGGING_LEVEL = logging.WARNING

    # The prefix used on every Redis key.
    REDIS_KEY_PREFIX = "carina:"

    REWARD_ISSUANCE_TASK_NAME = "reward-issuance"

    MESSAGE_IF_NO_PRE_LOADED_REWARDS: bool = False
    REWARD_ISSUANCE_REQUEUE_BACKOFF_SECONDS: int = 60 * 60 * 12  # 12 hours
    REWARD_STATUS_ADJUSTMENT_TASK_NAME = "reward-status-adjustment"

    PROMETHEUS_HTTP_SERVER_PORT: int = 9100

    TASK_MAX_RETRIES: int = 6
    TASK_RETRY_BACKOFF_BASE: float = 3.0
    TASK_QUEUE_PREFIX: str = "carina:"
    TASK_QUEUES: list[str] = None  # type: ignore [assignment]

    @validator("TASK_QUEUES", pre=True, always=True)
    @classmethod
    def task_queues(cls, v: list[str] | None, values: dict[str, Any]) -> list[str]:
        if v is not None:
            return v

        return [values["TASK_QUEUE_PREFIX"] + name for name in ("high", "default", "low")]

    PRE_LOADED_REWARD_BASE_URL: AnyHttpUrl
    JIGSAW_AGENT_USERNAME: str = "Bink_dev"
    JIGSAW_AGENT_PASSWORD: str = None  # type: ignore [assignment]

    @validator("JIGSAW_AGENT_PASSWORD", pre=True, always=True)
    @classmethod
    def fetch_jigsaw_agent_password(cls, v: str | None, values: dict[str, Any]) -> str:
        if v is not None:
            return v

        return values["KEY_VAULT"].get_secret("bpl-carina-agent-jigsaw-password")

    JIGSAW_AGENT_ENCRYPTION_KEY: str = None  # type: ignore [assignment]

    @validator("JIGSAW_AGENT_ENCRYPTION_KEY", pre=True, always=True)
    @classmethod
    def fetch_jigsaw_agent_encryption_key(cls, v: str | None, values: dict[str, Any]) -> str:
        if v is not None:
            return v

        return values["KEY_VAULT"].get_secret("bpl-carina-agent-jigsaw-encryption-key")

    REPORT_ANOMALOUS_TASKS_SCHEDULE = "*/10 * * * *"
    REPORT_TASKS_SUMMARY_SCHEDULE: str = "5,20,35,50 */1 * * *"
    REPORT_JOB_QUEUE_LENGTH_SCHEDULE: str = "*/10 * * * *"
    ACTIVATE_TASKS_METRICS: bool = True

    RABBITMQ_DSN: str = "amqp://guest:guest@localhost:5672//"
    MESSAGE_EXCHANGE_NAME: str = "hubble-activities"

    class Config:
        case_sensitive = True
        # env var settings priority ie priority 1 will override priority 2:
        # 1 - env vars already loaded (ie the one passed in by kubernetes)
        # 2 - env vars read from *local.env file
        # 3 - values assigned directly in the Settings class
        env_file = os.path.join(BASE_DIR, "local.env")
        env_file_encoding = "utf-8"


settings = Settings()
load_settings(settings)

dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "brief": {"format": "%(levelname)s:     %(asctime)s - %(message)s"},
            "console": {"()": "carina.core.reporting.ConsoleFormatter"},
            "json": {"()": "carina.core.reporting.JSONFormatter"},
        },
        "handlers": {
            "stderr": {
                "level": logging.NOTSET,
                "class": "logging.StreamHandler",
                "stream": sys.stderr,
                "formatter": settings.LOG_FORMATTER,
            },
            "stdout": {
                "level": logging.NOTSET,
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": settings.LOG_FORMATTER,
            },
        },
        "loggers": {
            "root": {
                "level": settings.ROOT_LOG_LEVEL or logging.INFO,
                "handlers": ["stdout"],
            },
            "uvicorn": {
                "propagate": False,
                "handlers": ["stdout"],
            },
            "sqlalchemy.engine": {
                "level": settings.QUERY_LOG_LEVEL or logging.WARN,
            },
            "alembic": {
                "level": "INFO",
                "handlers": ["stderr"],
                "propagate": False,
            },
        },
    }
)


# this will decode responses:
# >>> redis.set('test', 'hello')
# True
# >>> redis.get('test')
# 'hello'
redis = Redis.from_url(
    settings.REDIS_URL,
    socket_connect_timeout=3,
    socket_keepalive=True,
    retry_on_timeout=False,
    decode_responses=True,
)

# used for RQ:
# this will not decode responses:
# >>> redis.set('test', 'hello')
# True
# >>> redis.get('test')
# b'hello'
redis_raw = Redis.from_url(
    settings.REDIS_URL,
    socket_connect_timeout=3,
    socket_keepalive=True,
    retry_on_timeout=False,
)

if settings.SENTRY_DSN:  # pragma: no cover
    # by default sentry_sdk.init will integrate with logging and capture error messages as events
    # docs: https://docs.sentry.io/platforms/python/guides/logging/
    sentry_sdk.init(  # pylint: disable=abstract-class-instantiated
        dsn=settings.SENTRY_DSN,
        environment=settings.SENTRY_ENV,
        integrations=[
            RedisIntegration(),
        ],
        release=__version__,
        traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
    )
