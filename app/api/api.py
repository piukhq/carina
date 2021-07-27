from fastapi import APIRouter

from app.api.endpoints import voucher
from app.api.healthz import healthz_router
from app.core.config import settings

api_router = APIRouter()
api_router.include_router(healthz_router)
api_router.include_router(voucher.router, prefix=settings.API_PREFIX, tags=["voucher"])
