from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from fastapi_prometheus_metrics.endpoints import router as metrics_router
from fastapi_prometheus_metrics.manager import PrometheusManager
from fastapi_prometheus_metrics.middleware import MetricsSecurityMiddleware, PrometheusMiddleware
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from starlette.exceptions import HTTPException

from app.api.api import api_router
from app.core.config import settings
from app.core.exception_handlers import http_exception_handler, request_validation_handler, unexpected_exception_handler
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
    app.add_exception_handler(status.HTTP_500_INTERNAL_SERVER_ERROR, unexpected_exception_handler)

    app.add_middleware(MetricsSecurityMiddleware)
    app.add_middleware(PrometheusMiddleware)

    PrometheusManager(settings.PROJECT_NAME, metric_name_prefix="bpl")  # initialise signals

    if settings.SENTRY_DSN:
        app.add_middleware(SentryAsgiMiddleware)  # pragma: no cover

    # Prevent 307 temporary redirects if URLs have slashes on the end
    app.router.redirect_slashes = False

    return app
