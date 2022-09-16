from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from fastapi_prometheus_metrics.endpoints import router as metrics_router
from fastapi_prometheus_metrics.manager import PrometheusManager
from fastapi_prometheus_metrics.middleware import MetricsSecurityMiddleware, PrometheusMiddleware
from starlette.exceptions import HTTPException

from carina.api.api import api_router
from carina.core.config import settings
from carina.core.exception_handlers import (
    http_exception_handler,
    request_validation_handler,
    unexpected_exception_handler,
)
from carina.version import __version__


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

    # Prevent 307 temporary redirects if URLs have slashes on the end
    app.router.redirect_slashes = False

    return app
