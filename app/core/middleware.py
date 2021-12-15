from fastapi import status
from fastapi.requests import Request
from fastapi.responses import Response, UJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from app.core.config import settings


class MetricsSecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        local_port = request.scope["server"][1]
        if request.url.path == "/metrics" and local_port != 9100 and not settings.METRICS_DEBUG:
            return UJSONResponse({"detail": "Not found"}, status_code=status.HTTP_404_NOT_FOUND)
        return await call_next(request)
