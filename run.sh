#!/usr/bin/env bash
set -euo pipefail
if [ ! -f .env ]; then
    echo 'Copy .env.example to .env and generate the three secrets as described in README.md.'
    exit 1
fi
docker compose up --build -d --wait
