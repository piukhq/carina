from fastapi import APIRouter

from carina.api.endpoints import reward
from carina.api.healthz import healthz_router
from carina.core.config import settings

api_router = APIRouter()
api_router.include_router(healthz_router)
api_router.include_router(reward.router, prefix=settings.API_PREFIX, tags=["voucher"])
