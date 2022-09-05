from typing import Generator

import pytest

from cryptography.fernet import Fernet

from app.core.config import redis_raw, settings
from app.fetch_reward.jigsaw import Jigsaw


@pytest.fixture(scope="function", autouse=True)
def clean_redis() -> Generator:
    redis_raw.delete(Jigsaw.REDIS_TOKEN_KEY)
    yield
    redis_raw.delete(Jigsaw.REDIS_TOKEN_KEY)


@pytest.fixture(scope="module", autouse=True)
def populate_fernet_key() -> Generator:
    setattr(settings, "JIGSAW_AGENT_ENCRYPTION_KEY", Fernet.generate_key().decode())
    yield


@pytest.fixture(scope="module")
def fernet() -> Fernet:
    return Fernet(settings.JIGSAW_AGENT_ENCRYPTION_KEY.encode())
