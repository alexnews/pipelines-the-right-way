# Pipelines the Right Way — Chapter 01: Hello Assets (CSV → DuckDB)

**Goal:** Show how easy it is to start with Dagster using assets and a local DuckDB warehouse. No secrets, no paid services.

## Quick start (Docker)

```bash
bash docker/scripts/start.sh
# open http://localhost:3000
```

To stop:

```bash
bash docker/scripts/stop.sh
```

## Dagster telemetry
We ship a default `dagster.yaml` with telemetry disabled via `docker/dagster_home/dagster.yaml` and mount it to `/app/.dagster`.
