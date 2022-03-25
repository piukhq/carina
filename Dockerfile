FROM ghcr.io/binkhq/python:3.10

WORKDIR /app
ADD . .
RUN apt-get update && apt-get install gcc -y && \
    pipenv install --deploy --system --ignore-pipfile && \
    apt-get autoremove -y gcc && rm -rf /var/lib/apt/lists/*

ENV PROMETHEUS_MULTIPROC_DIR=/dev/shm
ENTRYPOINT [ "linkerd-await", "--" ]
CMD [ "gunicorn", "--workers=2", "--error-logfile=-", "--access-logfile=-", \
    "--worker-class=uvicorn.workers.UvicornWorker", \
    "--bind=0.0.0.0:9000", "--bind=0.0.0.0:9100", "asgi:app" ]
