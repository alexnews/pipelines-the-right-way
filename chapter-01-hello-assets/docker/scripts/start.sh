#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
COMPOSE_FILE="$SCRIPT_DIR/../docker-compose.yaml"
PROJECT_DIR="$( cd "$SCRIPT_DIR/../.." && pwd )"

echo ">> Starting Chapter 01 (Hello Assets)"
echo ">> Project: $PROJECT_DIR"
docker compose -f "$COMPOSE_FILE" -p ptwr-ch01 up --build
