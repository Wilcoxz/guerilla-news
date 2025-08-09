#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1
export DATABASE_URL=${DATABASE_URL:-"sqlite:///./events.sqlite"}
uvicorn services.api.app.main:app --reload --host 0.0.0.0 --port 8000
