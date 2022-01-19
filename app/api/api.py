from fastapi import APIRouter

from app.api.endpoints import reward
from app.api.healthz import healthz_router
from app.core.config import settings

api_router = APIRouter()
api_router.include_router(healthz_router)
api_router.include_router(reward.router, prefix=settings.API_PREFIX, tags=["voucher"])
