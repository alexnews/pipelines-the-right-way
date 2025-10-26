#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
COMPOSE_FILE="$SCRIPT_DIR/../docker-compose.yaml"

echo ">> Stopping Chapter 01 (Hello Assets)"
docker compose -f "$COMPOSE_FILE" -p ptwr-ch01 down
