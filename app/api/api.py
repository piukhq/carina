from fastapi import APIRouter

from app.api.healthz import healthz_router

api_router = APIRouter()
api_router.include_router(healthz_router)
