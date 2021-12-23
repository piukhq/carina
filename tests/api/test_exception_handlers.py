from typing import NoReturn
from unittest import mock

import pytest

from fastapi import FastAPI, status
from fastapi.testclient import TestClient

from app.core.exception_handlers import unexpected_exception_handler


@pytest.fixture(scope="function")
def exc() -> Exception:
    return ValueError("boom")


@pytest.fixture(scope="function")
def client(exc: Exception) -> TestClient:
    app = FastAPI()
    app.add_exception_handler(status.HTTP_500_INTERNAL_SERVER_ERROR, unexpected_exception_handler)

    @app.get("/boom")
    async def boom() -> NoReturn:
        raise exc

    return TestClient(app, raise_server_exceptions=False)


@mock.patch("app.core.exception_handlers.logger")
@mock.patch("app.core.exception_handlers.sentry_sdk")
def test_unexpected_exception_handler(
    mock_sentry_sdk: mock.MagicMock, mock_logger: mock.MagicMock, client: TestClient, exc: Exception
) -> None:
    resp = client.get("/boom")
    assert resp.json() == {
        "display_message": "An unexpected system error occurred, please try again later.",
        "code": "INTERNAL_ERROR",
    }
    mock_logger.exception.assert_called_once_with(exc)
    mock_sentry_sdk.capture_exception.assert_called_once_with(exc)
