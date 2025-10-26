# Chapter 01 - Hello Assets
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tini bash && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY tests /app/tests
COPY data /app/data
# Include default Dagster config (telemetry off) for non-compose runs
COPY docker/dagster_home /app/.dagster

EXPOSE 3000

ENV PYTHONPATH=/app/src \
    PTWR_DATA_DIR=/app/data \
    DAGSTER_HOME=/app/.dagster

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "pipelines_the_right_way.ch01.defs"]
