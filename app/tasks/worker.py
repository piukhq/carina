import click
import rq

from retry_tasks_lib.utils.error_handler import job_meta_handler

from app.core.config import redis, settings


@click.group()
def cli() -> None:
    pass


@cli.command()
def worker(burst: bool = False) -> None:

    # Placeholder for when we will implement Prometheus metrics in Carina
    # registry = prometheus_client.CollectorRegistry()
    # prometheus_client.multiprocess.MultiProcessCollector(registry)
    # prometheus_client.start_http_server(9100, registry=registry)

    worker = rq.Worker(
        queues=settings.TASK_QUEUES,
        connection=redis,
        log_job_description=True,
        exception_handlers=[job_meta_handler],
    )
    worker.work(burst=burst, with_scheduler=True)


if __name__ == "__main__":
    cli()
