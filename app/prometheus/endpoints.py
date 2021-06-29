from os import getenv
from typing import Any

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, CollectorRegistry, generate_latest, multiprocess

metrics_router = APIRouter()


@metrics_router.get(path="/metrics")
def handle_metrics() -> Any:  # pragma: no cover

    registry = REGISTRY
    if getenv("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)

    headers = {"Content-Type": CONTENT_TYPE_LATEST}
    return Response(generate_latest(registry), status_code=200, headers=headers)
