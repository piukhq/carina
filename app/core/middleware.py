import time

from blinker import signal
from fastapi import status
from fastapi.requests import Request
from fastapi.responses import Response, UJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from app.enums.event_signals import EventSignals


class MetricsSecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        local_port = request.scope["server"][1]
        if (local_port == 9100 and request.url.path != "/metrics") or (
            local_port != 9100 and request.url.path == "/metrics"
        ):
            return UJSONResponse({"detail": "Not found"}, status_code=status.HTTP_404_NOT_FOUND)
        return await call_next(request)


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:

        # Time our code
        before_time = time.perf_counter()
        response = await call_next(request)
        after_time = time.perf_counter()

        latency = after_time - before_time
        method = request.method
        signal(EventSignals.RECORD_HTTP_REQ).send(
            __name__,
            endpoint=request.url.path,
            latency=latency,
            response_code=response.status_code,
            method=method,
        )

        signal(EventSignals.INBOUND_HTTP_REQ).send(
            __name__, endpoint=request.url.path, response_code=response.status_code, method=method
        )

        return response
