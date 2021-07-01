FROM ghcr.io/binkhq/python:3.9

WORKDIR /app
ADD . .
RUN pipenv install --deploy --system --ignore-pipfile && \
    pipenv --clear

ENV PROMETHEUS_MULTIPROC_DIR=/dev/shm
CMD [ "gunicorn", "--workers=1", "--error-logfile=-", "--access-logfile=-", \
    "--worker-class=uvicorn.workers.UvicornWorker", \
    "--bind=0.0.0.0:9000", "asgi:app" ]
