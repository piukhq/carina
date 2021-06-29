import sentry_sdk

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from starlette.exceptions import HTTPException

from app.api.api import api_router
from app.core.config import settings
from app.core.exception_handlers import http_exception_handler, request_validation_handler
from app.core.middleware import MetricsSecurityMiddleware, PrometheusMiddleware
from app.prometheus.endpoints import metrics_router
from app.prometheus.manager import PrometheusManager
from app.version import __version__


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        openapi_url=f"{settings.API_PREFIX}/openapi.json",
    )
    app.include_router(api_router)
    app.include_router(metrics_router)
    app.add_exception_handler(RequestValidationError, request_validation_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)

    app.add_middleware(MetricsSecurityMiddleware)
    app.add_middleware(PrometheusMiddleware)

    PrometheusManager()  # initialise signals

    if settings.SENTRY_DSN:
        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            environment=settings.SENTRY_ENV,
            traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
            release=__version__,
        )
        app.add_middleware(SentryAsgiMiddleware)

    # Prevent 307 temporary redirects if URLs have slashes on the end
    app.router.redirect_slashes = False

    return app
