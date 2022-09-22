import logging

from typing import TYPE_CHECKING, Any, Callable

from prometheus_client import Counter, Gauge, Histogram

from carina.core.config import settings

if TYPE_CHECKING:  # pragma: no cover
    from requests import RequestException, Response  # pragma: no cover

logger = logging.getLogger(__name__)

METRIC_NAME_PREFIX = "bpl_"

outgoing_http_requests_total = Counter(
    name=f"{METRIC_NAME_PREFIX}outgoing_http_requests_total",
    documentation="Total outgoing http requests by response status.",
    labelnames=("app", "method", "response", "exception", "url"),
)

tasks_run_total = Counter(
    name=f"{METRIC_NAME_PREFIX}tasks_run_total",
    documentation="Counter for tasks run.",
    labelnames=("app", "task_name"),
)

task_statuses = Gauge(
    name=f"{METRIC_NAME_PREFIX}task_anomalies",
    documentation="The current number of tasks in an unusual state",
    labelnames=("app", "task_name", "status"),
)

tasks_summary = Gauge(
    name=f"{METRIC_NAME_PREFIX}task_summary",
    documentation="The current number of tasks and their status",
    labelnames=("app", "task_name", "status"),
)

job_queue_summary = Gauge(
    name=f"{METRIC_NAME_PREFIX}job_queue_length",
    documentation="The current number of jobs in each RQ queue",
    labelnames=("app", "queue_name"),
)

tasks_processing_time_histogram = Histogram(
    name=f"{METRIC_NAME_PREFIX}tasks_processing_time",
    documentation="Total time taken by a task to process",
    labelnames=("app", "task_name"),
)


def update_metrics_hook(url_label: str) -> Callable:  # pragma: no cover
    # pylint: disable=unused-argument
    def update_metrics(resp: "Response", *args: Any, **kwargs: Any) -> None:
        outgoing_http_requests_total.labels(
            app=settings.PROJECT_NAME,
            method=resp.request.method,
            response=f"HTTP_{resp.status_code}",
            exception=None,
            url=url_label,
        ).inc()

    return update_metrics


def update_metrics_exception_handler(ex: "RequestException", method: str, url: str) -> None:  # pragma: no cover
    outgoing_http_requests_total.labels(
        app=settings.PROJECT_NAME,
        method=method,
        response=None,
        exception=ex.__class__.__name__,
        url=url,
    ).inc()


def task_processing_time_callback_fn(task_processing_time: float, task_name: str) -> None:
    logger.info(f"Updating {tasks_processing_time_histogram} metrics...")
    tasks_processing_time_histogram.labels(app=settings.PROJECT_NAME, task_name=task_name).observe(task_processing_time)
