import logging
import os

import typer

from prometheus_client import CollectorRegistry
from prometheus_client import start_http_server as start_prometheus_server
from prometheus_client import values
from prometheus_client.multiprocess import MultiProcessCollector
from retry_tasks_lib.reporting import report_anomalous_tasks, report_queue_lengths, report_tasks_summary
from retry_tasks_lib.utils.error_handler import job_meta_handler
from rq import Worker

from carina.core.config import redis_raw, settings
from carina.db.session import SyncSessionMaker
from carina.imports.agents.file_agent import RewardImportAgent, RewardUpdatesAgent
from carina.scheduled_tasks.scheduler import cron_scheduler as carina_cron_scheduler
from carina.scheduled_tasks.task_cleanup import cleanup_old_tasks
from carina.tasks.prometheus import job_queue_summary, task_statuses, tasks_summary

cli = typer.Typer()
logger = logging.getLogger(__name__)


@cli.command()
def task_worker(burst: bool = False) -> None:
    if settings.ACTIVATE_TASKS_METRICS:
        # -------- this is the prometheus monkey patch ------- #
        values.ValueClass = values.MultiProcessValue(os.getppid)
        # ---------------------------------------------------- #
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        logger.info("Starting prometheus metrics server...")
        start_prometheus_server(settings.PROMETHEUS_HTTP_SERVER_PORT, registry=registry)

    worker = Worker(
        queues=settings.TASK_QUEUES,
        connection=redis_raw,
        log_job_description=True,
        exception_handlers=[job_meta_handler],
    )
    logger.info("Starting task worker...")
    worker.work(burst=burst, with_scheduler=True)


@cli.command()
def cron_scheduler(
    imports: bool = True,
    updates: bool = True,
    report_tasks: bool = True,
    report_rq_queues: bool = True,
    task_cleanup: bool = True,
) -> None:  # pragma: no cover

    logger.info("Initialising scheduler...")
    if imports:
        carina_cron_scheduler.add_job(
            RewardImportAgent().do_import,
            schedule_fn=lambda: settings.BLOB_IMPORT_SCHEDULE,
            coalesce_jobs=True,
        )

    if updates:
        carina_cron_scheduler.add_job(
            RewardUpdatesAgent().do_import,
            schedule_fn=lambda: settings.BLOB_IMPORT_SCHEDULE,
            coalesce_jobs=True,
        )

    if report_tasks:
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        logger.info("Starting prometheus metrics server...")
        start_prometheus_server(settings.PROMETHEUS_HTTP_SERVER_PORT, registry=registry)

        carina_cron_scheduler.add_job(
            report_anomalous_tasks,
            kwargs={"session_maker": SyncSessionMaker, "project_name": settings.PROJECT_NAME, "gauge": task_statuses},
            schedule_fn=lambda: settings.REPORT_ANOMALOUS_TASKS_SCHEDULE,
            coalesce_jobs=True,
        )
        carina_cron_scheduler.add_job(
            report_tasks_summary,
            kwargs={
                "session_maker": SyncSessionMaker,
                "project_name": settings.PROJECT_NAME,
                "gauge": tasks_summary,
            },
            schedule_fn=lambda: settings.REPORT_TASKS_SUMMARY_SCHEDULE,
            coalesce_jobs=True,
        )

    if report_rq_queues:
        carina_cron_scheduler.add_job(
            report_queue_lengths,
            kwargs={
                "redis": redis_raw,
                "project_name": settings.PROJECT_NAME,
                "queue_names": settings.TASK_QUEUES,
                "gauge": job_queue_summary,
            },
            schedule_fn=lambda: settings.REPORT_JOB_QUEUE_LENGTH_SCHEDULE,
            coalesce_jobs=True,
        )

    if task_cleanup:
        carina_cron_scheduler.add_job(
            cleanup_old_tasks,
            schedule_fn=lambda: settings.TASK_CLEANUP_SCHEDULE,
            coalesce_jobs=True,
        )

    logger.info(f"Starting scheduler {carina_cron_scheduler}...")
    carina_cron_scheduler.run()


@cli.callback()
def callback() -> None:
    """
    carina command line interface
    """


if __name__ == "__main__":
    cli()
